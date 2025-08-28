# sql_console

Class for seamlessly interacting with SQL systems from Python at Apex

Installation:

    pip install git+git://github.apexclearing.local/rlinehan/sql_console

It's a fairly simple wrapper that allows one line connection to any SQL server:

    from sql_console.sql_console import SqlWrapper
    luna = SqlWrapper({'env': 'prd', 'method': 'pyodbc', 'server': 'luna', 'credentials': {'user': 'username', 'password': 'password'}, 'debug': True, 'format': 'json'})
    
Connection without 'credentials' JSON key will automatically attempt connection using current AD user credentials, domain APEXCLEARING is automatically assumed:

    from sql_console.sql_console import SqlWrapper
    luna = SqlWrapper({'env': 'prd', 'method': 'pyodbc', 'server': 'luna', 'debug': True, 'format': 'json'})

One line querying with a list of records as output:

    results = luna.query({'db': 'Ale', 'query': 'SELECT * FROM table', 'results': True})

Some queries do not need to return results:

    results = luna.query({'db': 'Ale', 'query': 'INSERT INTO table (column) VALUES("value")', 'results': False})

To add new instance aliases for the 'server' parameter, just edit the sql_console/hosts.py file with desired key/host pair in the correct environment. If you don't need to alias the server you're connecting to, just use the hostname as it automatically checks sql_console/hosts.py and uses the given string if it cannot find a matching alias.

Supported values for 'method' parameter:

* pyodbc (ODBC connector)

* pymysql (MySQL)

* psycopg2 (Postgres)

* pymssql (SQL Server - not well tested, just use pyodbc)

SqlWrapper.proc() lets you call stored procedures:

    results = luna.proc({'proc': 'dbo.usp_StoredProcedure', 'params': (arg1,arg2,)})

Queries return list of results if successful, else boolean False. Queries without results (INSERTs, UPDATEs, etc.) return boolean True. Set 'debug' parameter to True for verbose output.