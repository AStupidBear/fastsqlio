import datetime
import platform
import re
import connectorx as cx

import pandas as pd
from pandahouse import read_clickhouse, to_clickhouse
from sql_metadata import Parser
from sqlalchemy import MetaData


def read_sql(sql, con, chunksize=None, port_shift=-877, **kwargs):
    if con.engine.name == "clickhouse":
        port = con.url.port + port_shift
        connection = {
            "host": "http://" + con.url.host + ":" + str(port), 
            "database": con.url.database,
            "user": con.url.username,
            "password": con.url.password
        }
        df = read_clickhouse(sql, connection=connection, chunksize=chunksize, **kwargs)
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
        if platform.processor() == "aarch64" or chunksize:
            df = pd.read_sql(sql, con, chunksize=chunksize, **kwargs)
        else:
            url = re.sub("\+\w+(?=:)", "", str(con.engine.url))
            df = cx.read_sql(url, sql, **kwargs)
        def transform(df):
            for c in df.columns:
                if dtypes[c] == datetime.date:
                    df[c] = pd.to_datetime(df[c])
                elif dtypes[c] == datetime.time:
                    df[c] = pd.to_timedelta(df[c])
            return df
    return map(transform, df) if chunksize else transform(df)


def to_sql(df, name, con, port_shift=-877, **kwargs):
    if con.engine.name == "clickhouse":
        port = con.url.port + port_shift
        connection = {
            "host": "http://" + con.url.host + ":" + str(port), 
            "database": con.url.database,
            "user": con.url.username,
            "password": con.url.password
        }
        for c in df.columns:
            if df[c].dtype.name.startswith("timedelta"):
                df[c] = df[c].dt.total_seconds().mul(1e6).astype("int64")
        to_clickhouse(df, name, connection=connection, **kwargs)
    else:
        df.to_sql(name, con, if_exists="append", **kwargs)

