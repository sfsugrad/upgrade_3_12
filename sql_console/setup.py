from setuptools import setup

setup(
    name='sql_console',
    version='0.1',
    packages=['sql_console'],
    install_requires=[
        "pyodbc",
        "pymssql",
        "pymysql",
        "psycopg2",
    ],
    python_requires=">=3.8",
)