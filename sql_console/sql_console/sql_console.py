"""Database connectivity helper utilities."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence

import pyodbc
import pymssql
import pymysql
import psycopg2

from .hosts import db


DRIVER_ERRORS: tuple[type[Exception], ...] = (
    pyodbc.Error,
    pymssql.Error,
    pymysql.Error,
    psycopg2.Error,
)


class SqlWrapperConnectionError(Exception):
    """Raised when the wrapper cannot establish a connection to the database."""


class SqlWrapper:
    """Wrap a handful of database client libraries with a unified interface."""

    def __init__(self, param: Dict[str, Any]):
        self.env = param['env']
        self.server = param['server']
        self.debug = param.get('debug', False)
        self.format = param.get('format')
        self.method = param['method']

        if self.server not in db[self.env]:
            db[self.env][self.server] = self.server

        self.c: Dict[str, Dict[str, Any]] = {self.env: {}}

        credentials = param.get('credentials') or {}

        if self.debug:
            print('SqlWrapper.init: info: connecting to ' + db[self.env][self.server])

        try:
            connection = self._create_connection(param, credentials)
        except DRIVER_ERRORS as connection_error:
            raise SqlWrapperConnectionError(
                'SqlWrapper.init: error: could not connect to '
                + db[self.env][self.server]
                + ' with user '
                + credentials.get('user', '<unknown>')
                + ': message: '
                + str(connection_error)
            ) from connection_error

        self.c[self.env][self.server] = connection

        if self.method not in ['pymssql', 'pymysql']:
            connection.autocommit = True

        self.cursor = self._create_cursor(connection)

    def __enter__(self) -> 'SqlWrapper':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        self.c[self.env][self.server].close()

    def _create_connection(self, param: Dict[str, Any], credentials: Dict[str, Any]):
        method = self.method
        database = param.get('db')

        if method in {'dsn', 'pymssql', 'pymysql'}:
            self._validate_credentials(credentials, method)

        if method == 'pyodbc':
            return self._connect_pyodbc(credentials, database)
        if method == 'dsn':
            return pyodbc.connect(
                'DSN=' + self.server
                + ';UID='
                + credentials['user']
                + ';PWD='
                + credentials['password']
                + ';trusted_connection=yes'
            )
        if method == 'pymssql':
            return self._connect_pymssql(credentials, database)
        if method == 'pymysql':
            return self._connect_pymysql(credentials, database)
        if method == 'psycopg2':
            return self._connect_psycopg2(credentials, database)

        raise SqlWrapperConnectionError('SqlWrapper.init: error: method "' + method + '" is not supported')

    def _create_cursor(self, connection):
        if self.method == 'pymysql':
            return connection.cursor(pymysql.cursors.DictCursor)
        if self.method == 'pymssql':
            return connection.cursor(as_dict=True)
        return connection.cursor()

    def _connect_pyodbc(self, credentials: Dict[str, Any], database: str | None):
        connection_string = self._build_pyodbc_connection_string(credentials, database, '{SQL Server}')
        try:
            return pyodbc.connect(connection_string, autocommit=True)
        except pyodbc.Error as pyodbc_error:
            if self.debug:
                print(
                    'SqlWrapper.init.pyodbc: error: could not connect to '
                    + db[self.env][self.server]
                    + ': message: '
                    + str(pyodbc_error)
                )
                print('SqlWrapper.init.pyodbc: info: attempting to connect with FreeTDS driver...')
            freetds_connection_string = self._build_pyodbc_connection_string(credentials, database, '{FreeTDS}')
            return pyodbc.connect(freetds_connection_string, autocommit=True)

    def _build_pyodbc_connection_string(self, credentials: Dict[str, Any], database: str | None, driver: str) -> str:
        server = db[self.env][self.server]
        parts = [f'DRIVER={driver}', f'SERVER={server}']

        if driver == '{SQL Server}':
            parts.append('PORT=1433')

        user = credentials.get('user')
        password = credentials.get('password')

        if user and password:
            parts.append(f'UID=APEXCLEARING\\{user}')
            parts.append(f'PWD={password}')

        if database:
            parts.append(f'DATABASE={database}')

        parts.append('trusted_connection=yes')
        return ';'.join(parts)

    def _connect_pymssql(self, credentials: Dict[str, Any], database: str | None):
        kwargs = {
            'server': db[self.env][self.server],
            'user': 'APEXCLEARING\\' + credentials['user'],
            'password': credentials['password'],
            'autocommit': True,
        }
        if database:
            kwargs['database'] = database
        return pymssql.connect(**kwargs)

    def _connect_pymysql(self, credentials: Dict[str, Any], database: str | None):
        connection_kwargs = {
            'host': db[self.env][self.server],
            'user': credentials['user'],
            'password': credentials['password'],
            'autocommit': True,
        }
        if database:
            connection_kwargs['database'] = database

        return pymysql.connect(**connection_kwargs)

    def _connect_psycopg2(self, credentials: Dict[str, Any], database: str | None):
        if not database:
            raise SqlWrapperConnectionError('SqlWrapper.init.psycopg2: error: "db" parameter is required for psycopg2 connections')

        connection_kwargs: Dict[str, Any] = {
            'dbname': database,
            'host': db[self.env][self.server],
            'port': 5432,
        }
        if 'user' in credentials:
            connection_kwargs['user'] = credentials['user']
        if 'password' in credentials:
            connection_kwargs['password'] = credentials['password']

        return psycopg2.connect(**connection_kwargs)

    def _validate_credentials(self, credentials: Dict[str, Any], method: str) -> None:
        if 'user' not in credentials or 'password' not in credentials:
            raise SqlWrapperConnectionError(
                f'SqlWrapper.init.{method}: error: credentials with "user" and "password" are required'
            )

    def query(self, param: Dict[str, Any]):
        if 'db' in param:
            if self.debug:
                print('SqlWrapper.query: info: switching database to "' + str(param['db']) + '"')
            try:
                self._switch_database(param['db'])
            except SqlWrapperConnectionError as switch_error:
                if self.debug:
                    print('SqlWrapper.query: error: ' + str(switch_error))
                return False

        if 'results' not in param:
            if self.debug:
                print('SqlWrapper.query: error: expecting "results" parameter')
            return False

        query_definition = param['query']
        results_requested = param['results'] is True
        dict_requested = param.get('dict') is True

        if isinstance(query_definition, list):
            output: List[List[Any]] = []
            for query_string in query_definition:
                try:
                    self.cursor.execute(query_string)
                except DRIVER_ERRORS as query_error:
                    if self.debug:
                        print('SqlWrapper.query: error: query failed: ' + str(query_error))
                    return False
                rows = self.cursor.fetchall()
                output.append([row[0] for row in rows])
            return output if results_requested else True

        if isinstance(query_definition, str):
            if self.debug:
                print('SqlWrapper.query: info: executing query')

            try:
                parameters = param.get('parameters')
                if parameters is not None:
                    self.cursor.execute(query_definition, parameters)
                else:
                    self.cursor.execute(query_definition)
            except DRIVER_ERRORS as query_error:
                if self.debug:
                    print('SqlWrapper.query: error: query failed: ' + str(query_error))
                return False

            return self._fetch_query_results(results_requested, dict_requested)

        if self.debug:
            print('SqlWrapper.query: error: "query" parameter invalid, expecting list or string')
        return False

    def _switch_database(self, database: Any) -> None:
        database_name = self._validate_database_name(database)

        if self.method in {'pyodbc', 'dsn', 'pymssql'}:
            statement = f'USE {self._quote_identifier(database_name, "bracket")}'
        elif self.method == 'pymysql':
            statement = f'USE {self._quote_identifier(database_name, "backtick")}'
        elif self.method == 'psycopg2':
            raise SqlWrapperConnectionError('SqlWrapper.query: error: psycopg2 connections do not support switching databases')
        else:
            statement = 'USE ' + database_name

        try:
            self.cursor.execute(statement)
        except DRIVER_ERRORS as switch_error:
            raise SqlWrapperConnectionError(
                'SqlWrapper.query: error: failed to switch database to "'
                + database_name
                + '": '
                + str(switch_error)
            ) from switch_error

    def _validate_database_name(self, database: Any) -> str:
        if not isinstance(database, str):
            raise SqlWrapperConnectionError('SqlWrapper.query: error: database name must be a string')
        if not database.strip():
            raise SqlWrapperConnectionError('SqlWrapper.query: error: database name must not be empty')
        return database

    def _quote_identifier(self, identifier: str, style: str) -> str:
        if style == 'bracket':
            return '[' + identifier.replace(']', ']]') + ']'
        if style == 'backtick':
            return '`' + identifier.replace('`', '``') + '`'
        return identifier

    def _fetch_query_results(self, results_requested: bool, dict_requested: bool):
        if not results_requested:
            return True

        if self.cursor.description is None:
            return True

        rows = self.cursor.fetchall()

        if self.method in {'pyodbc', 'dsn'} and dict_requested:
            return self._rows_as_dicts(rows)

        return rows

    def _rows_as_dicts(self, rows: Iterable[Sequence[Any]]) -> List[Dict[str, Any]]:
        columns = [column[0] for column in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def proc(self, param: Dict[str, Any]):
        procedure = param['proc']
        parameters: Sequence[Any] = param.get('params', [])
        as_dict = param.get('dict') is True

        try:
            if self.method in {'pyodbc', 'dsn'}:
                return self._execute_pyodbc_proc(procedure, parameters, as_dict)
            if self.method == 'pymssql':
                return self._execute_pymssql_proc(procedure, parameters)
        except DRIVER_ERRORS as proc_error:
            if self.debug:
                print('SqlWrapper.proc: error: proc failed: ' + str(proc_error))
            return False

        if self.debug:
            print('SqlWrapper.proc: error: method "' + self.method + '" is not supported')
        return False

    def _execute_pyodbc_proc(self, procedure: str, parameters: Sequence[Any], as_dict: bool):
        placeholders = ', '.join('?' for _ in parameters)
        call = f'{{CALL {procedure}}}' if not placeholders else f'{{CALL {procedure} ({placeholders})}}'
        if self.debug:
            print('SqlWrapper.proc: executing query: ' + call + ', ' + str(tuple(parameters)))
        self.cursor.execute(call, parameters)

        if self.cursor.description is None:
            return True

        rows = self.cursor.fetchall()
        if as_dict:
            return self._rows_as_dicts(rows)
        return rows

    def _execute_pymssql_proc(self, procedure: str, parameters: Sequence[Any]):
        self.cursor.callproc(procedure, parameters)
        results: List[List[Any]] = []

        while True:
            try:
                rows = self.cursor.fetchall()
            except pymssql.ProgrammingError:
                rows = []
            if rows:
                results.append(rows)
            if not self.cursor.nextset():
                break

        if not results:
            return True
        if len(results) == 1:
            return results[0]
        return results
