class SqlWrapperConnectionError(Exception):
    pass


class SqlWrapper():

    def __init__(self, param):
        import pyodbc
        import pymssql
        import pymysql
        import psycopg2
        from .hosts import db

        self.env = param['env']
        self.server = param['server']
        self.debug = param['debug']
        self.format = param['format']
        self.method = param['method']

        if self.server not in db[self.env]:
            db[self.env][self.server] = self.server

        self.c = {self.env: {}}
        try:
            if self.debug:
                print('SqlWrapper.init: info: connecting to ' + db[self.env][self.server])
            if self.method == 'pyodbc':
                try:
                    if 'credentials' in param:
                        if 'db' in param:
                            self.c[self.env][self.server] = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db[self.env][self.server] + ';PORT=1443;UID=APEXCLEARING\\' + param['credentials']['user'] + ';PWD=' + param['credentials']['password'] + ';DATABASE=' + param['db'] + ';trusted_connection=yes', autocommit=True)
                        else:
                            self.c[self.env][self.server] = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db[self.env][self.server] + ';PORT=1443;UID=APEXCLEARING\\' + param['credentials']['user'] + ';PWD=' + param['credentials']['password'] + ';trusted_connection=yes', autocommit=True)
                    else:
                        if 'db' in param:
                            self.c[self.env][self.server] = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db[self.env][self.server] + ';PORT=1443;DATABASE=' + param['db'] + ';trusted_connection=yes', autocommit=True)
                        else:
                            self.c[self.env][self.server] = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db[self.env][self.server] + ';PORT=1443;trusted_connection=yes', autocommit=True)
                except pyodbc.Error as pyodbcerr:
                    if self.debug:
                        print('SqlWrapper.init.pyodbc: error: could not connect to ' + db[self.env][self.server] + ': message: ' + str(pyodbcerr))
                        print('SqlWrapper.init.pyodbc: info: attempting to connect with FreeTDS driver...')
                    # linux workaround for connecting to SQL server via pyodbc and FreeTDS
                    try:
                        if 'credentials' in param:
                            self.c[self.env][self.server] = pyodbc.connect('DRIVER={FreeTDS};SERVER=' + db[self.env][self.server] + ';UID=APEXCLEARING\\' + param['credentials']['user'] + ';PWD=' + param['credentials']['password'] + ';trusted_connection=yes')
                        else:
                            self.c[self.env][self.server] = pyodbc.connect('DRIVER={FreeTDS};SERVER=' + db[self.env][self.server] + ';trusted_connection=yes')
                    except pyodbc.Error as pyodbcerr:
                        raise SqlWrapperConnectionError('SqlWrapper.init.pyodbc: error: could not connect to ' + db[self.env][self.server] + ' with user ' + param['credentials']['user'] + ': message: ' + str(pyodbcerr))

            elif self.method == 'dsn':
                self.c[self.env][self.server] = pyodbc.connect('DSN=' + self.server + ';UID=' + param['credentials']['user'] + ';PWD=' + param['credentials']['password'] + ';trusted_connection=yes')
            elif self.method == 'pymssql':
                if 'db' in param:
                    self.c[self.env][self.server] = pymssql.connect(server=db[self.env][self.server], user='APEXCLEARING\\'+param['credentials']['user'], password=param['credentials']['password'], database=param['db'], autocommit=True)
                else:
                    self.c[self.env][self.server] = pymssql.connect(server=db[self.env][self.server], user='APEXCLEARING\\' + param['credentials']['user'], password=param['credentials']['password'], autocommit=True)
            elif self.method == 'pymysql':
                self.c[self.env][self.server] = pymysql.connect(host=db[self.env][self.server], user=param['credentials']['user'], password=param['credentials']['password'], autocommit=True)
            #elif self.method == 'mysqlclient':
                #self.c[self.env][self.server] = myc.connect(param['credentials']['user'], param['credentials']['password'], host=db[self.env][self.server], buffered=True)
            elif self.method == 'psycopg2':
                if 'credentials' in param:
                    self.c[self.env][self.server] = psycopg2.connect(dbname=param['db'], user=param['credentials']['user'], password=param['credentials']['password'], host=db[self.env][self.server], port=5432)
                else:
                    self.c[self.env][self.server] = psycopg2.connect(dbname=param['db'], user=param['credentials']['user'], password=param['credentials']['password'], host=db[self.env][self.server], port=5432)
        except pyodbc.Error as PyPyODBC:
            raise SqlWrapperConnectionError('SqlWrapper.init.pyodbc: error: could not connect to ' + db[self.env][self.server] + ' with user ' + param['credentials']['user'] + ': message: ' + str(PyPyODBC))
        except pymssql.Error as sqlerror:
            raise SqlWrapperConnectionError('SqlWrapper.init.pymssql: error: could not connect to ' + db[self.env][self.server] + ' with user ' +param['credentials']['user'] + ': message: ' + str(sqlerror))
        except pymysql.Error as sqlerror:
            raise SqlWrapperConnectionError('SqlWrapper.init.pymysql: error: could not connect to ' + db[self.env][self.server] + ' with user ' +param['credentials']['user'] + ': message: ' + str(sqlerror))
        #except myc.Error as mycerror:
            #if self.debug:
                #print('SqlWrapper.init.mysqlclient: error: could not connect to ' + db[self.env][self.server] + ' with user ' +param['credentials']['user'] + ': message: ' + str(mycerror))
        except psycopg2.Error as psycopg2err:
            raise SqlWrapperConnectionError('SqlWrapper.init.psycopg2: error: could not connect to ' + db[self.env][self.server] + ' with user ' +param['credentials']['user'] + ': message: ' + str(psycopg2err))

        # call autocommit method for certain connection methods
        if self.method not in ['pymssql', 'pymysql']:
            self.c[self.env][self.server].autocommit = True

        # initialize cursors - return results as dictionary
        if self.method == 'pymysql':
            self.cursor = self.c[self.env][self.server].cursor(pymysql.cursors.DictCursor)
        elif self.method == 'pymssql':
            self.cursor = self.c[self.env][self.server].cursor(as_dict=True)
        else:
            self.cursor = self.c[self.env][self.server].cursor()

    def _rows_to_dicts(self, rows):
        """Return query results as dictionaries keyed by column name."""
        description = self.cursor.description
        if not description:
            return []

        column_names = [column[0] for column in description]
        return [dict(zip(column_names, row)) for row in rows]

    def __exit__(self):
        self.c[self.env][self.server].close()

    def close(self):
        self.c[self.env][self.server].close()

    def query(self, param):
        if 'db' in param:
            if self.debug:
                print('SqlWrapper.query: info: switching database to "' + param['db'] + '"')
            try:
                self.cursor.execute('USE ' + param['db'])
            except self.cursor.Error as cerr:
                if self.debug:
                    print('SqlWrapper.query: error: query failed: ' + str(cerr))
                return False

        if 'results' in param:
            if isinstance(param['query'], list):
                output = []
                for q in param['query']:
                    try:
                        self.cursor.execute(q)
                    except Exception as cerr:
                        if self.debug:
                            print('SqlWrapper.query: error: query failed: ' + str(cerr))
                        return False
                    output.append([i[0] for i in self.cursor.fetchall()])
                if param['results'] is True:
                    return output
                else:
                    return True

            elif isinstance(param['query'], str):
                if self.debug:
                    print('SqlWrapper.query: info: executing query')

                try:
                    self.cursor.execute(param['query'])
                except Exception as cerr:
                    if self.debug:
                        print('SqlWrapper.query: error: query failed: ' + str(cerr))
                    return False

                if self.method == 'psycopg2':
                    # description method will be None if the query does not return results
                    if self.cursor.description is None:
                        return True
                    else:
                        if param['results'] is True:
                            return self.cursor.fetchall()
                        else:
                            return True

                # TODO: cheating with ''dict' in param' because current code is not compatible - should be 'param['dict'] is True'
                # https://stackoverflow.com/a/27422384/2237552
                elif self.method == 'pyodbc' and 'dict' in param:
                    if param['results'] is True:
                        return self._rows_to_dicts(self.cursor.fetchall())
                    else:
                        return True
                else:
                    if param['results'] is True:
                        return self.cursor.fetchall()
                    else:
                        return True

            else:
                if self.debug:
                    print('SqlWrapper.query: error: "query" parameter invalid, expecting list or string')
                return False
        else:
            if self.debug:
                print('SqlWrapper.query: error: expecting "results" parameter')
            return False

    def proc(self, param):
        import pymssql, pyodbc

        if self.method == 'pyodbc':
            try:
                if self.debug:
                    print('SqlWrapper.proc: executing query: {CALL ' + param['proc'] + ' (' + str(''.join(['?,' for i in param['params']]))[:-1] + ')}, ' + str(param['params']))
                self.cursor.execute('{CALL ' + param['proc'] + ' (' + str(''.join(['?,' for i in param['params']]))[:-1] + ')}', param['params'])
                return self._rows_to_dicts(self.cursor.fetchall())
            except pyodbc.Error as cerr:
                if self.debug:
                    print('SqlWrapper.proc: error: proc failed: ' + str(cerr))
                return False

        #TODO: pymssql callproc is not working correctly - not sure why
        elif self.method == 'pymssql':
            try:
                self.cursor.callproc(param['proc'], param['params'])
                results = self.cursor.fetchall()
                while results:
                    if self.cursor.nextset():
                        results.append(self.cursor.fetchall())
                    else:
                        return results
            except pymssql.Error as cerr:
                if self.debug:
                    print('SqlWrapper.proc: error: proc failed: ' + str(cerr))

        else:
            if self.debug:
                print('SqlWrapper.proc: error: method "' + self.method + '" is not supported')
            return None
