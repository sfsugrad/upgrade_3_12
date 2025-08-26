from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from sql_console.sql_console import SqlWrapper


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Gets SOD extract runs from ReportRequest and ships them to Postgres"
    )
    parser.add_argument(
        "--environment",
        dest="environment",
        type=str,
        default=None,
        help="Environment: [prod][uat][dev]",
    )
    parser.add_argument(
        "--process-date",
        dest="process_date",
        type=str,
        default=None,
        help="YYYY-MM-DD",
    )
    parser.add_argument(
        "--postgres-username",
        dest="username",
        type=str,
        default=None,
        help="Username for Postgres",
    )
    parser.add_argument(
        "--postgres-password",
        dest="password",
        type=str,
        default=None,
        help="Password for Postgres",
    )
    parser.add_argument(
        "--notgucci-argument-001",
        dest="flag_001",
        action="store_true",
        help="Stupid workaround for EXT001",
    )
    parser.set_defaults(flag_001=False)
    return parser.parse_args()


def read_sql(path: str) -> str:
    """Read a SQL file and collapse newlines."""
    return Path(path).read_text(encoding="utf-8").replace("\n", " ")


def main() -> None:
    args = parse_args()
    # calculate next day from given process date
    nextday = datetime.strptime(args.process_date, "%Y-%m-%d") + timedelta(days=1)

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

    # query for all extracts excluding EXT001
    sod_extracts = read_sql("sql/sod_extracts.sql").replace(
        "[[PROCESSDATE]]", f"'{args.process_date}'"
    )
    sod_extracts_results = apollo.query({"query": sod_extracts, "results": True})

    if args.flag_001:
        # query for EXT001
        sod_001 = read_sql("sql/sod_extract_001.sql").replace(
            "[[PROCESSDATE]]", f"'{nextday.strftime('%Y-%m-%d')}'"
        )
        sod_001_results = apollo.query({"query": sod_001, "results": True})
        if sod_001_results:
            sod_extracts_results.extend(sod_001_results)

    extracts_already_in_postgres = [
        i[0]
        for i in batch.query(
            {
                "query": (
                    "SELECT extract FROM batch.sod_extract_runs "
                    f"WHERE process_date='{args.process_date}'"
                ),
                "results": True,
            }
        )
    ]

    for r in sod_extracts_results:
        if r[1] not in extracts_already_in_postgres:
            if r[1] == "001":
                batch.query(
                    {
                        "query": (
                            "INSERT INTO batch.sod_extract_runs "
                            "(process_date,extract,end_time) VALUES("
                            f"'{nextday.strftime('%Y-%m-%d %H:%M:%S')}', "
                            f"'EXT{r[1]}', "
                            f"'{r[0].strftime('%Y-%m-%d %H:%M:%S')}'"
                            ")"
                        ),
                        "results": False,
                    }
                )
            else:
                batch.query(
                    {
                        "query": (
                            "INSERT INTO batch.sod_extract_runs "
                            "(process_date,extract,end_time) VALUES("
                            f"'{args.process_date}', "
                            f"'EXT{r[1]}', "
                            f"'{r[0].strftime('%Y-%m-%d %H:%M:%S')}'"
                            ")"
                        ),
                        "results": False,
                    }
                )


if __name__ == "__main__":
    main()

