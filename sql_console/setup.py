from distutils.core import setup

setup(
    name='sql_console',
    version='0.1',
    packages=['sql_console'],
    install_requires=[
        'pymysql',
        'psycopg2',
        'sqlalchemy',
        'psycopg2',
    ]
)