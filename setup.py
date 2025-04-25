from setuptools import setup, find_packages

setup(
    name="fastsqlio",
    version="0.1.0",
    description="Fast read_sql and to_sql",
    packages=find_packages(),
    install_requires=["pandas", "sqlalchemy==1.4.48", "pymysql", "sql_metadata", "connectorx; python_version > '3.6'",
                      "clickhouse-driver[lz4]@git+https://github.com/AStupidBear/clickhouse-driver.git@extensionarray",
                      "clickhouse-sqlalchemy@git+https://github.com/AStupidBear/clickhouse-sqlalchemy.git@dtprec"]
)
