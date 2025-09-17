from sql_console.sql_console import sql_console


class DummyCursor:
    description = None

    def execute(self, *args, **kwargs):
        pass

    def fetchall(self):
        return []

    def nextset(self):
        return False


class DummyConnection:
    def __init__(self, cursor_class):
        self._cursor_class = cursor_class

    def cursor(self, cursor_class):
        assert cursor_class is self._cursor_class
        return DummyCursor()

    def close(self):
        pass


def test_pymysql_connection_uses_database(monkeypatch):
    captured_kwargs = {}

    def fake_connect(**kwargs):
        captured_kwargs.update(kwargs)
        return DummyConnection(sql_console.pymysql.cursors.DictCursor)

    monkeypatch.setattr(sql_console.pymysql, 'connect', fake_connect)

    wrapper = sql_console.SqlWrapper(
        {
            'env': 'prd',
            'server': 'apollo',
            'method': 'pymysql',
            'db': 'Ale',
            'credentials': {'user': 'user', 'password': 'pass'},
        }
    )

    try:
        assert captured_kwargs['database'] == 'Ale'
    finally:
        wrapper.close()
