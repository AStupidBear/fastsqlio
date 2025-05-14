from setuptools import setup, find_packages

setup(
    name="fastsqlio",
    version="0.2.0",
    description="Fast read_sql and to_sql",
    packages=find_packages(),
    install_requires=["pandas", "sqlalchemy", "pymysql", "sql_metadata", "connectorx; python_version > '3.6'", "clickhouse-driver[lz4]", "clickhouse-sqlalchemy"]
)
