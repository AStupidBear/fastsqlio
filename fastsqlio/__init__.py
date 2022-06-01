import datetime
import importlib
import platform
import re
from numbers import Number

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


def convert(pytype, value):
    if isinstance(value, datetime.datetime) and pytype == datetime.date:
        return value.date()
    else:
        return value


def sqlquote(value):
    if isinstance(value, Number):
        return str(value)
    else:
        return "'" + str(value) + "'"


def drop_duplicates(df, name, con, category_keys=[], range_keys=[]):
    if not re.match("clickhouse|duckdb", con.engine.url.drivername):
        return df
    meta = MetaData(con)
    meta.reflect()
    table = meta.tables[name]
    if con.engine.url.drivername.startswith("clickhouse"):
        primary_keys = table.engine.primary_key.expressions
    else:
        info = read_sql(f"PRAGMA table_info('{name}')", con)
        primary_keys = info.query("pk == True")["name"].to_list()
    sql = f"SELECT {','.join(primary_keys)} FROM {name}"
    conditions = []
    for key in category_keys:
        pytype = table.columns[key].type.python_type
        uniq = ",".join([sqlquote(convert(pytype, x)) for x in df[key].unique()])
        conditions.append(f"({key} IN (" + uniq + "))")
    for key in range_keys:
        pytype = table.columns[key].type.python_type
        vmin = sqlquote(convert(pytype, df[key].min()))
        vmax = sqlquote(convert(pytype, df[key].max()))
        conditions.append(f"({key} BETWEEN {vmin} AND {vmax})")
    if len(conditions) > 0:
        sql += " WHERE " + " AND ".join(conditions)
    df_sql = read_sql(sql, con)
    if len(df_sql) == 0:
        return df
    if len(primary_keys) > 1:
        index = pd.MultiIndex.from_frame(df_sql)
    else:
        index = df_sql[primary_keys[0]]
    return df.set_index(primary_keys).drop(index=index, errors="ignore").reset_index()


def query_dataframe(
        self, query, params=None, external_tables=None, query_id=None,
        settings=None):
    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError('Extras for NumPy must be installed')

    data, columns = self.execute(
        query, columnar=True, with_column_types=True, params=params,
        external_tables=external_tables, query_id=query_id,
        settings=settings
    )

    return pd.DataFrame(
        {col[0]: d for d, col in zip(data, columns)}
    )


def read_sql(sql, con, chunksize=None, port_shift=0, **kwargs):
    url = con.engine.url
    if url.drivername.startswith("clickhouse"):
        if url.drivername == "clickhouse+native":
            client = con.connection.connection.transport
            df = query_dataframe(client, sql)
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
                if c not in dtypes:
                    continue
                if dtypes[c] == datetime.date:
                    df[c] = pd.to_datetime(df[c])
                elif dtypes[c] == datetime.time:
                    df[c] = pd.to_timedelta(df[c])
            return df
    return map(transform, df) if chunksize else transform(df)


def to_sql(df, name, con, port_shift=0, index=False, if_exists="append", keys=None, dtype=None, clengine="ReplacingMergeTree()", ignore_duplicate=True, category_keys=[], range_keys=[], **kwargs):
    if len(df) == 0:
        return
    url = con.engine.url
    if index:
        df = df.reset_index()
    if url.drivername.startswith("clickhouse"):
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        mycon = create_engine(re.sub("clickhouse\+\w+(?=:)", "mysql+pymysql", str(url)))
        schema = pd.io.sql.get_schema(df, name, keys, mycon, dtype)
        schema = re.sub(".*(?=PRIMARY)", "", schema)
        schema += " ENGINE = " + clengine
        schema = re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", schema)
        con.execute(schema)
        if ignore_duplicate:
            df = drop_duplicates(df, name, con, category_keys, range_keys)
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
            to_clickhouse(df, name, connection=connection, index=False, **kwargs)
    elif url.drivername.startswith("duckdb"):
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        schema = pd.io.sql.get_schema(df, name, keys, con, dtype)
        schema = re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", schema)
        con.execute(schema)
        if ignore_duplicate:
            df = drop_duplicates(df, name, con, category_keys, range_keys)
        conn = con.connection.c
        conn.from_df(df).insert_into(name)
        # conn.register("df", df)
        # conn.execute(f"INSERT INTO {name} SELECT * FROM df")
    else:
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].add(pd.Timestamp(0)).dt.time
        schema = pd.io.sql.get_schema(df, name, keys, con, dtype)
        schema = re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", schema)
        con.execute(schema)
        if ignore_duplicate and not event.contains(con, "before_cursor_execute", ignore_insert):
            event.listen(con, "before_cursor_execute", ignore_insert, retval=True)
        df.to_sql(name, con, index=False, if_exists=if_exists, dtype=dtype, **kwargs)
