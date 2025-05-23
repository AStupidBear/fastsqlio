import datetime
import re
from numbers import Number

import pandas as pd
from clickhouse_sqlalchemy import engines
from clickhouse_sqlalchemy.drivers.compilers.ddlcompiler import \
    ClickHouseDDLCompiler
from pandas.io.sql import get_schema
from sql_metadata import Parser
from sqlalchemy import MetaData, create_engine, event, text
from sqlalchemy.engine import Engine

_post_create_table = ClickHouseDDLCompiler.post_create_table


def post_create_table(self, table, *args, **kwargs):
    if not hasattr(table, 'engine'):
        table.engine = engines.ReplacingMergeTree(primary_key=table.primary_key)
    text = _post_create_table(self, table, *args, **kwargs)
    return text


ClickHouseDDLCompiler.post_create_table = post_create_table


def to_conn(con):
    if type(con) == str:
        con = create_engine(con).connect()
    elif type(con) == Engine:
        con = con.connect()
    return con


def get_table(con, name, schema=None):
    meta = MetaData(schema=schema)
    meta.reflect(bind=con)
    if schema is None:
        return meta.tables[name]
    else:
        return meta.tables[f"{schema}.{name}"]


def create_table(df, name, keys, con, dtype, schema=None):
    """Create table using SQLAlchemy if it doesn't exist."""
    sql = get_schema(df, name, keys=keys, con=con, dtype=dtype, schema=schema)
    con.execute(text(sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")))


def drop_duplicates(df, name, con, schema=None, category_keys=[], range_keys=[]):
    if not re.match("clickhouse|duckdb", con.engine.url.drivername):
        return df
    table = get_table(con, name, schema=schema)
    df0 = df.copy()
    
    # Use SQLAlchemy to get primary keys consistently across database types
    primary_keys = [col.name for col in table.primary_key.columns]
    
    if not primary_keys:
        # Fallback for engines that might not properly register primary keys in metadata
        if con.engine.url.drivername.startswith("clickhouse"):
            primary_keys = [c.name for c in table.engine.primary_key.columns]
        elif con.engine.url.drivername.startswith("duckdb"):
            info = read_sql(f"PRAGMA table_info('{name}')", con)
            primary_keys = info.query("pk == True")["name"].to_list()
    
    if not primary_keys:
        return df

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

    fields = ','.join(['"' + k + '"' for k in primary_keys])
    table_name = name if schema is None else f"{schema}.{name}"
    sql = f"SELECT {fields} FROM {table_name}"
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


def query_dataframe(self, query, params=None, external_tables=None, query_id=None, settings=None):
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
        "Date": "datetime64[s]",
        "Date32": "datetime64[s]",
        "DateTime": "datetime64[s]",
        "DateTime64": "datetime64[ns]",
        "Nullable(Date)": "datetime64[s]",
        "Nullable(Date32)": "datetime64[s]",
        "Nullable(DateTime)": "datetime64[s]",
        "Nullable(DateTime64)": "datetime64[ns]",
    }
    if len(data) > 0:
        dtype = {col[0]: ch_pd_type_map[col[1]] for col in columns if col[1] in ch_pd_type_map and col[1].startswith("Nullable")}
    else:
        dtype = {col[0]: ch_pd_type_map[col[1]] for col in columns if col[1] in ch_pd_type_map}
    columns = [col[0] for col in columns]
    data = {col: d for d, col in zip(data, columns)}
    df = pd.DataFrame(data=data, columns=columns)
    df = df.astype(dtype, errors="ignore")
    return df


def read_sql(sql, con, chunksize=None, **kwargs):
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
            table = get_table(con, name, schema=schema)
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


def to_sql(df, name, con, schema=None, if_exists="append", index=False, keys=None, dtype=None, ignore_duplicate=True, category_keys=[], range_keys=[], **kwargs):
    if len(df) == 0:
        return
    con = to_conn(con)
    url = con.engine.url
    if not schema:
        if "." in name:
            schema, name = name.split(".")
    table_name = name if schema is None else f"{schema}.{name}"
    if index:
        df = df.reset_index()
    if url.drivername.startswith("clickhouse"):
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        if keys is None:
            keys = category_keys + range_keys
        create_table(df, name, keys, con, dtype, schema)
        if ignore_duplicate:
            df = drop_duplicates(df, name, con, schema, category_keys, range_keys)
        if url.drivername == "clickhouse+native":
            client = con.connection.connection.transport
            columns = '"' + df.columns + '"'
            client.insert_dataframe(f"INSERT INTO {table_name} ({','.join(columns)}) VALUES", df)
        else:
            raise NotImplementedError(f"{url.drivername} is not supported yet")
    elif url.drivername.startswith("duckdb"):
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        create_table(df, name, keys, con, dtype, schema)
        if ignore_duplicate:
            df = drop_duplicates(df, name, con, schema, category_keys, range_keys)
        conn = con.connection.c
        conn.from_df(df).insert_into(table_name)
        # conn.register("df", df)
        # conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    else:
        for c in df.columns[df.dtypes == "timedelta64[ns]"]:
            df[c] = df[c].add(pd.Timestamp(0)).dt.time
        create_table(df, name, keys, con, dtype, schema)

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

        if ignore_duplicate and not event.contains(con, "before_cursor_execute", ignore_insert):
            event.listen(con, "before_cursor_execute", ignore_insert, retval=True)
        df.to_sql(name, con, schema=schema, index=False, if_exists=if_exists, dtype=dtype, **kwargs)
