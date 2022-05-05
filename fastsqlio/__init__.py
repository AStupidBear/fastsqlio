import datetime
import importlib
import platform
import re

import pandas as pd
from pandahouse import read_clickhouse, to_clickhouse
from sql_metadata import Parser
from sqlalchemy import MetaData, create_engine, event


def ignore_insert(conn, cursor, statement, parameters, context, executemany):
    if "INSERT INTO " not in statement:
        return statement, parameters
    if conn.engine.name == "mysql":
        statement = statement.replace("INSERT INTO ", "INSERT IGNORE INTO ")
    elif conn.engine.name == "postgresql":
        statement = statement.replace("INSERT INTO ", "INSERT ON CONFLICT IGNORE INTO ")
    elif conn.engine.name == "sqlite":
        statement = statement.replace("INSERT INTO ", "INSERT OR IGNORE INTO ")
    return statement, parameters


def read_sql(sql, con, chunksize=None, port_shift=0, **kwargs):
    url = con.engine.url
    if url.drivername.startswith("clickhouse"):
        if url.drivername == "clickhouse+native":
            client = con.connection.connection.transport
            df = client.query_dataframe(sql)
            df = df if chunksize is None else [df]
        else:
            port = url.port + port_shift
            connection = {
                "host": "http://" + url.host + ":" + str(port), 
                "database": url.database,
                "user": url.username,
                "password": url.password
            }
            df = read_clickhouse(sql, connection=connection, chunksize=chunksize, **kwargs)
        def transform(df):
            for c in df.columns:
                if re.match("^time$", c, re.IGNORECASE) and \
                    df[c].dtype.name == "int64":
                    df[c] = pd.to_timedelta(df[c], unit="us")
            return df
    elif url.drivername.startswith("duckdb"):
        c = con.connection.c
        query = c.execute(sql)
        if chunksize is None:
            df = c.fetchdf()
        else:
            def fetch_df_chunk(query, con):
                while True:
                    df = query.fetch_df_chunk()
                    if len(df) == 0:
                        break
                    yield df
            df = fetch_df_chunk(query, c)
        def transform(df):
            for c in df.columns:
                if re.match("^time$", c, re.IGNORECASE) and \
                    df[c].dtype.name == "int64":
                    df[c] = pd.to_timedelta(df[c], unit="us")
            return df
    else:
        tables = Parser(sql).tables
        meta = MetaData(con)
        meta.reflect(only=tables)
        dtypes = {}
        for t in tables:
            for c in meta.tables[t].columns:
                dtypes[c.name] = c.type.python_type
        connectorx_spec = importlib.util.find_spec("connectorx")
        if platform.processor() == "aarch64" or chunksize or connectorx_spec is None:
            df = pd.read_sql(sql, con, chunksize=chunksize, **kwargs)
        else:
            import connectorx as cx
            df = cx.read_sql(re.sub("\+\w+(?=:)", "", str(url)), sql, **kwargs)
        def transform(df):
            for c in df.columns:
                if dtypes[c] == datetime.date:
                    df[c] = pd.to_datetime(df[c])
                elif dtypes[c] == datetime.time:
                    df[c] = pd.to_timedelta(df[c])
            return df
    return map(transform, df) if chunksize else transform(df)


def to_sql(df, name, con, port_shift=0, index=False, if_exists="append", keys=None, dtype=None, clengine="ReplacingMergeTree()", compression="false", ignore_duplicate=True, **kwargs):
    url = con.engine.url
    if url.drivername.startswith("clickhouse"):
        mycon = create_engine(re.sub("clickhouse\+\w+(?=:)", "mysql", str(url)))
        schema = pd.io.sql.get_schema(df, name, keys, mycon, dtype)
        schema = re.sub(".*(?=PRIMARY)", "", schema)
        schema += " ENGINE = " + clengine
    else:
        schema = pd.io.sql.get_schema(df, name, keys, con, dtype)
    schema = re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", schema)
    con.execute(schema)
    if url.drivername.startswith("clickhouse"):
        if url.drivername == "clickhouse+native":
            client = con.connection.connection.transport
            client.insert_dataframe(f"INSERT INTO {name} VALUES", df)
        else:
            port = url.port + port_shift
            connection = {
                "host": "http://" + url.host + ":" + str(port), 
                "database": url.database,
                "user": url.username,
                "password": url.password
            }
            for c in df.columns:
                if df[c].dtype.name.startswith("timedelta"):
                    df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
            to_clickhouse(df, name, connection=connection, index=index, **kwargs)
    elif url.drivername.startswith("duckdb"):
        for c in df.columns:
            if df[c].dtype.name.startswith("timedelta"):
                df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        c = con.connection.c
        c.from_df(df).insert_into(name)
        # c.register("df", df)
        # c.execute(f"INSERT INTO {name} SELECT * FROM df")
    else:
        if url.drivername.startswith("mysql"):
            for c in df.columns:
                if df[c].dtype.name.startswith("timedelta"):
                    df[c] = df[c].add(pd.Timestamp(0)).dt.time
        if ignore_duplicate and not event.contains(con, "before_cursor_execute", ignore_insert):
            event.listen(con, "before_cursor_execute", ignore_insert, retval=True)
        df.to_sql(name, con, index=index, if_exists=if_exists, dtype=dtype, **kwargs)
