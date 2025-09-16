from setuptools import find_packages, setup

setup(
    name='sql_console',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pymysql',
        'psycopg2',
        'sqlalchemy',
    ],
)
