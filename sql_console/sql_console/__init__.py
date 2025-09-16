"""Top-level package for sql_console."""

from .sql_console import SqlWrapper, SqlWrapperConnectionError

__all__ = ["SqlWrapper", "SqlWrapperConnectionError"]
