#!/usr/bin/env python3
"""Calculate service level objective data and insert into batch database."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

from sql_console.sql_console import SqlWrapper


def main() -> int:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--process-date",
        dest="process_date",
        type=str,
        default=None,
        help="YYYY-MM-DD",
    )
    parser.add_argument(
        "--environment",
        dest="environment",
        type=str,
        default=None,
        help="environment [dev][uat][prd]",
    )
    parser.add_argument(
        "--postgres-username",
        dest="username",
        type=str,
        default=None,
        help="Postgres username",
    )
    parser.add_argument(
        "--postgres-password",
        dest="password",
        type=str,
        default=None,
        help="Postgres password",
    )

    args = parser.parse_args()

    apollo = SqlWrapper(
        {
            "env": args.environment,
            "method": "pyodbc",
            "server": "apollo",
            "db": "worldwide",
            "debug": True,
            "format": "json",
        }
    )

    batch = SqlWrapper(
        {
            "env": args.environment,
            "method": "psycopg2",
            "server": f"pg{args.environment}",
            "db": "batch",
            "credentials": {"user": args.username, "password": args.password},
            "debug": True,
            "format": "json",
        }
    )

    process_date = datetime.strptime(args.process_date, "%Y-%m-%d")
    slo_date = process_date + timedelta(days=1)
    dow_int = process_date.weekday()

    constant_name: str | None = None

    match dow_int:
        case 4:
            constant_name = "monthly_oe" if 15 <= process_date.day <= 21 else "friday"
        case 0 | 1 | 2 | 3:
            constant_name = "monday-thursday"

    if constant_name is not None:
        slos = apollo.query(
            {
                "query": (
                    "select * from ConstantValueLookup where ApplicationName='batch_slo' "
                    f"and ConstantName='{constant_name}'"
                ),
                "results": True,
            }
        )

        for r in slos:
            slo = f"{slo_date:%Y-%m-%d} {r[3]}"
            result = batch.query(
                {
                    "query": (
                        "INSERT INTO batch.slos (process_date, slo) "
                        f"VALUES ('{args.process_date}', '{slo}')"
                    ),
                    "results": False,
                }
            )
            if result is not True:
                print(
                    f"calculate_slos.py: error: INSERT query failed with state: {result}"
                )
                apollo.close()
                batch.close()
                return 1

        apollo.close()
        batch.close()
        return 0

    print(
        "calculate_slos.py: error: either the process_date is on a weekend, "
        "or something is very wrong with calculating ConstantName... input arguments: "
        f"{args}"
    )
    apollo.close()
    batch.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())

