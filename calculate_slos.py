"""Populate the batch.slos table for a given process date."""

from __future__ import annotations

import argparse
import datetime
import sys
from collections.abc import Sequence

from sql_console.sql_console import SqlWrapper, SqlWrapperConnectionError


def parse_process_date(value: str) -> datetime.date:
    """Parse ``value`` into a :class:`datetime.date`.

    ``argparse`` calls this helper for the ``--process-date`` option.  Using
    :meth:`datetime.date.fromisoformat` keeps the accepted format aligned with
    ISO-8601 while providing a clear error message when the input is invalid.
    """

    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - exercised via argparse
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Return validated command line arguments."""

    parser = argparse.ArgumentParser(
        description="Calculate batch SLO records for the provided process date.",
    )

    parser.add_argument(
        "--process-date",
        dest="process_date",
        type=parse_process_date,
        required=True,
        help="Process date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--environment",
        dest="environment",
        type=str,
        required=True,
        help="Target environment (dev, uat, prd).",
    )
    parser.add_argument(
        "--postgres-username",
        dest="username",
        type=str,
        required=True,
        help="Postgres username.",
    )
    parser.add_argument(
        "--postgres-password",
        dest="password",
        type=str,
        required=True,
        help="Postgres password.",
    )

    return parser.parse_args(argv)


def determine_constant_name(process_date: datetime.date) -> str | None:
    """Return the ConstantValueLookup key for ``process_date``.

    The legacy "Scott method" determines which constant name should be used for
    the calculated day.  Only weekdays are eligible for SLOs; weekends return
    ``None`` to indicate that no insert should occur.
    """

    weekday = process_date.weekday()

    if weekday == 4:  # Friday
        if 15 <= process_date.day <= 21:
            return "monthly_oe"
        return "friday"

    if 0 <= weekday <= 3:
        return "monday-thursday"

    return None


def coerce_slo_time(value: object) -> str:
    """Convert ``value`` returned from the database into a time string."""

    if isinstance(value, datetime.time):
        return value.strftime("%H:%M:%S")
    return str(value)


def escape_sql_literal(value: str) -> str:
    """Escape single quotes for a SQL literal."""

    return value.replace("'", "''")


def main(argv: Sequence[str] | None = None) -> int:
    """Program entry point."""

    args = parse_args(argv)

    process_date: datetime.date = args.process_date
    environment = args.environment.lower()
    username = args.username
    password = args.password

    constant_name = determine_constant_name(process_date)
    if constant_name is None:
        print(
            "calculate_slos.py: error: the provided process_date"
            f" {process_date.isoformat()} falls on a weekend or does not have"
            " an SLO configuration."
        )
        return 1

    slo_date = process_date + datetime.timedelta(days=1)

    try:
        apollo = SqlWrapper(
            {
                "env": environment,
                "method": "pyodbc",
                "server": "apollo",
                "db": "worldwide",
                "debug": True,
                "format": "json",
            }
        )
        batch = SqlWrapper(
            {
                "env": environment,
                "method": "psycopg2",
                "server": f"pg{environment}",
                "db": "batch",
                "credentials": {"user": username, "password": password},
                "debug": True,
                "format": "json",
            }
        )
    except ModuleNotFoundError as exc:
        print(
            "calculate_slos.py: error: required database driver"
            f" '{exc.name}' is not installed for Python {sys.version.split()[0]}."
        )
        return 1
    except SqlWrapperConnectionError as exc:
        print(f"calculate_slos.py: error: {exc}")
        return 1

    try:
        constant_query = (
            "select * from ConstantValueLookup "
            "where ApplicationName='batch_slo' "
            f"and ConstantName='{constant_name}'"
        )
        slos = apollo.query({"query": constant_query, "results": True})

        if slos is False:
            print(
                "calculate_slos.py: error: failed to fetch SLO configuration"
                " from Apollo."
            )
            return 1

        if not slos:
            print(
                "calculate_slos.py: error: no SLO configuration found for"
                f" constant '{constant_name}'."
            )
            return 1

        process_date_sql = escape_sql_literal(process_date.isoformat())
        inserted_rows = 0

        for row in slos:
            if len(row) <= 3:
                print(
                    "calculate_slos.py: error: unexpected row format returned"
                    " from Apollo."
                )
                return 1

            slo_timestamp = (
                f"{slo_date.isoformat()} {coerce_slo_time(row[3]).strip()}"
            )
            slo_timestamp_sql = escape_sql_literal(slo_timestamp)

            insert_query = (
                "INSERT INTO batch.slos (process_date, slo) "
                f"VALUES ('{process_date_sql}', '{slo_timestamp_sql}')"
            )
            result = batch.query({"query": insert_query, "results": False})

            if result is not True:
                print(
                    "calculate_slos.py: error: INSERT query failed with state: "
                    f"{result}"
                )
                return 1

            inserted_rows += 1

        if inserted_rows == 0:
            print(
                "calculate_slos.py: warning: no SLO rows were inserted."
            )

        return 0

    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"calculate_slos.py: error: unexpected failure: {exc}")
        return 1

    finally:
        try:
            apollo.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass

        try:
            batch.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass


if __name__ == "__main__":  # pragma: no cover - script entry point
    sys.exit(main())
