#!/usr/bin/env python3
"""Load Tidal job results into Grafana.

This script queries a Tidal database for generated SQL and executes the
returned statements against a Postgres instance.  The original code targeted
older versions of Python and contained a number of bugs (for example, a typo in
the ``origin`` argument handling).  It has been modernised for Python 3.12 and
restructured for clarity.

Usage example::

    python tidal_to_grafana_v2.py \
        --process-date 2024-01-01 \
        --environment dev \
        --postgres-username USER \
        --postgres-password PASS \
        --query my_query.sql \
        --origin ozark
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from sql_console.sql_console import SqlWrapper


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser()

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
        help="Environment: [dev][uat][prd]",
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

    parser.add_argument(
        "--query",
        dest="query",
        type=str,
        default=None,
        help="Filename that contains relevant query",
    )

    parser.add_argument(
        "--origin",
        dest="origin",
        type=str,
        default=None,
        help="Server where data originates",
    )

    parser.add_argument(
        "--query-parameters",
        dest="query_parameters",
        type=str,
        default=None,
        help="CSV list of parameters to replace placeholders in specified query",
    )

    return parser.parse_args()


def build_connection(origin: str, env: str) -> SqlWrapper:
    """Return a :class:`SqlWrapper` configured for ``origin``."""

    match origin:
        case "ozark":
            return SqlWrapper(
                {
                    "env": env,
                    "method": "pyodbc",
                    "server": "ozark",
                    "db": "admiral",
                    "debug": True,
                    "format": "json",
                }
            )

        case "eagle":
            return SqlWrapper(
                {
                    "env": env,
                    "method": "pyodbc",
                    "server": "eagle",
                    "db": "tradeking",
                    "debug": True,
                    "format": "json",
                }
            )

        case "hood":
            return SqlWrapper(
                {
                    "env": env,
                    "method": "pyodbc",
                    "server": "hood",
                    "db": "fbidb",
                    "debug": True,
                    "format": "json",
                }
            )

        case "apollo":
            return SqlWrapper(
                {
                    "env": env,
                    "method": "pyodbc",
                    "server": "apollo",
                    "db": "worldwide",
                    "debug": True,
                    "format": "json",
                }
            )

        case _:
            raise ValueError(f"Unknown origin: {origin}")


def main() -> int:
    args = parse_args()

    connection = build_connection(args.origin, args.environment)

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

    # Parse query parameters into correct format
    params: list[str] | None = None
    if args.query_parameters is not None:
        params = args.query_parameters.split(",")

    script_path = Path("sql") / args.query
    with script_path.open("rt", encoding="utf-8") as f:
        tidal_script = f.read().replace("\n", " ")

    if "[[PROCESSDATE]]" in tidal_script and args.process_date:
        tidal_script = tidal_script.replace("[[PROCESSDATE]]", f"'{args.process_date}'")

    if params is not None:
        for index, p in enumerate(params):
            placeholder = f"[[{index}]]"
            if placeholder in tidal_script:
                tidal_script = tidal_script.replace(placeholder, f"'{p}'")

    print(f"tidal_to_grafana: info: tidal query: {tidal_script}")

    tidal_source_results = connection.query({"query": tidal_script, "results": True})
    tidal_source_results = [i[0] for i in tidal_source_results]  # tuple to list

    if not tidal_source_results:
        print("tidal_to_grafana: error: script returned no INSERT records...")
        return 1

    print("tidal source results:")
    for sr in tidal_source_results:
        if not sr:
            return 1

        print(str(sr))
        dest_results = batch.query({"query": str(sr), "results": True})
        if dest_results is False:
            print(f"tidal_to_grafana: error: destination query failed: {sr}")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

