from setuptools import setup, find_packages

setup(
    name="fastsqlio",
    version="0.1.0",
    description="Fast read_sql and to_sql",
    packages=find_packages(),
    install_requires=["pandas", "sqlalchemy", "pymysql", "sql_metadata", "connectorx; python_version > '3.6'",
                      "pandahouse@git+https://github.com/AStupidBear/pandahouse.git@parquet"],
)
