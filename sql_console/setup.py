
from setuptools import setup

setup(
    name='sql_console',
    version='0.3.1',
    author='Rory Linehan',
    author_email='rlinehan@apexclearing.com',
    packages=find_packages(include=['sql_console']),
    # scripts=['bin/stowe-towels.py','bin/wash-towels.py'],
    url='http://github.apexclearing.local/rlinehan/sql_console',
    # license='LICENSE.txt',
    description='Package for connecting seamlessly to SQL systems at Apex',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',  # Ensure rendering of Markdown content
    install_requires=[
        "pyodbc==4.0.16",
        "pymssql",
        "pymysql",
        # "mysql.connector",  # Uncomment if needed
        "psycopg2",
    ],
main
)