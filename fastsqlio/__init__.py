import datetime
import re
from numbers import Number

import pandas as pd
from sql_metadata import Parser
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.engine import Engine


def to_conn(con):
    if type(con) == str:
        con = create_engine(con).connect()
    elif type(con) == Engine:
        con = con.connect()
    return con


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


def get_table(con, name):
    db = split_dbtbl(name)[0]
    meta = MetaData(con, db)
    meta.reflect()
    return meta.tables[name]


def split_dbtbl(name):
    if "." in name:
        return name.split(".")
    else:
        return None, name

        
def get_schema(df, name, keys, con, dtype):
    url = con.engine.url
    if url.drivername.startswith("clickhouse"):
        con = create_engine(str(url).split("?")[0])
    db, tbl = split_dbtbl(name)
    from inspect import signature
    if len(signature(pd.io.sql.get_schema).parameters) == 5:
        sql = pd.io.sql.get_schema(df, tbl, keys=keys, con=con, dtype=dtype)
    else:
        sql = pd.io.sql.get_schema(df, tbl, keys=keys, con=con, dtype=dtype, schema=db)
    sql = re.sub("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", sql)
    return sql


def drop_duplicates(df, name, con, category_keys=[], range_keys=[]):
    if not re.match("clickhouse|duckdb", con.engine.url.drivername):
        return df
    table = get_table(con, name)
    if con.engine.url.drivername.startswith("clickhouse"):
        primary_keys = [c.name for c in table.engine.primary_key.columns]
    else:
        info = read_sql(f"PRAGMA table_info('{name}')", con)
        primary_keys = info.query("pk == True")["name"].to_list()
    fields = ','.join(['"' + k + '"' for k in primary_keys])
    sql = f"SELECT {fields} FROM {name}"
    conditions = []
    for key in category_keys:
        pytype = table.columns[key].type.python_type
        uniq = ",".join([sqlquote(convert(pytype, x)) for x in df[key].unique()])
        conditions.append(f'("{key}" IN ({uniq}))')
    for key in range_keys:
        pytype = table.columns[key].type.python_type
        vmin = sqlquote(convert(pytype, df[key].min()))
        vmax = sqlquote(convert(pytype, df[key].max()))
        conditions.append(f'("{key}" BETWEEN {vmin} AND {vmax})')
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
    ch_pd_type_map = {
        "UInt8": "uint8",
        "UInt16": "uint16",
        "UInt32": "uint32",
        "UInt64": "uint64",
        "Int8": "int8",
        "Int16": "int16",
        "Int32": "int32",
        "Int64": "int64",
        "Float32": "float32",
        "Float64": "float64",
        "Decimal": "float64",
        "Date": "datetime64[ns]",
        "Date32": "datetime64[ns]",
        "DateTime": "datetime64[ns]",
        "DateTime64": "datetime64[ns]",
    }
    dtype = {col[0]: ch_pd_type_map[col[1]] for col in columns if col[1] in ch_pd_type_map}
    columns = [col[0] for col in columns]
    data = {col: d for d, col in zip(data, columns)}
    df = pd.DataFrame(data=data, columns=columns)
    if df.empty:
        df = df.astype(dtype)
    return df


def trasform_time(df):
    for c in df.columns:
        if re.match("^time$", c, re.IGNORECASE) and \
            df[c].dtype.name == "int64":
            df[c] = pd.to_timedelta(df[c], unit="us")
    return df


def read_sql(sql, con, chunksize=None, port_shift=0, **kwargs):
    con = to_conn(con)
    if chunksize is not None:
        con = con.execution_options(stream_results=True, max_row_buffer=chunksize)
    url = con.engine.url
    def trasform_time(df):
        for c in df.columns:
            if re.match("^time$", c, re.IGNORECASE) and \
                df[c].dtype.name == "int64":
                df[c] = pd.to_timedelta(df[c], unit="us")
        return df
    if url.drivername == "clickhouse+native":
        if chunksize is None:
            client = con.connection.connection.transport
            df = query_dataframe(client, sql)
        else:
            df = pd.read_sql(sql, con, chunksize=chunksize, **kwargs)
        transform = trasform_time
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
        transform = trasform_time
    else:
        dtypes = {}
        for name in Parser(sql).tables:
            table = get_table(con, name)
            for c in table.columns:
                dtypes[c.name] = c.type.python_type
        try:
            import connectorx as cx
            badcx = False
        except ImportError:
            badcx = True
        if chunksize or badcx:
            df = pd.read_sql(sql, con, chunksize=chunksize, **kwargs)
        else:
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


def to_sql(df, name, con, port_shift=0, index=False, if_exists="append", keys=None, dtype=None, ignore_duplicate=True, category_keys=[], range_keys=[], **kwargs):
    if len(df) == 0:
        return
    con = to_conn(con)
    url = con.engine.url
    if index:
        df = df.reset_index()
    if url.drivername.startswith("clickhouse"):
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        if keys is None:
            keys = category_keys + range_keys
        con.execute(get_schema(df, name, keys, con, dtype))
        if ignore_duplicate:
            df = drop_duplicates(df, name, con, category_keys, range_keys)
        if url.drivername == "clickhouse+native":
            client = con.connection.connection.transport
            columns = '"' + df.columns + '"'
            client.insert_dataframe(f"INSERT INTO {name} ({','.join(columns)}) VALUES", df)
        else:
            raise NotImplementedError(f"{url.drivername} is not supported yet")
    elif url.drivername.startswith("duckdb"):
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        con.execute(get_schema(df, name, keys, con, dtype))
        if ignore_duplicate:
            df = drop_duplicates(df, name, con, category_keys, range_keys)
        conn = con.connection.c
        conn.from_df(df).insert_into(name)
        # conn.register("df", df)
        # conn.execute(f"INSERT INTO {name} SELECT * FROM df")
    else:
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].add(pd.Timestamp(0)).dt.time
        con.execute(get_schema(df, name, keys, con, dtype))
        if ignore_duplicate and not event.contains(con, "before_cursor_execute", ignore_insert):
            event.listen(con, "before_cursor_execute", ignore_insert, retval=True)
        df.to_sql(name, con, index=False, if_exists=if_exists, dtype=dtype, **kwargs)
