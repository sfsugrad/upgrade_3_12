from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional


SQL_DIR = Path(__file__).resolve().parent / "sql"
EXT_001 = "001"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
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
    return parser.parse_args(argv)


def read_sql(path: Path) -> str:
    """Read a SQL file and collapse newlines."""
    try:
        return Path(path).read_text(encoding="utf-8").replace("\n", " ")
    except OSError as exc:
        raise FileNotFoundError(f"Unable to read SQL file: {path}") from exc


def main() -> None:
    from sql_console.sql_console import SqlWrapper

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
    try:
        sod_extracts = read_sql(SQL_DIR / "sod_extracts.sql")
        sod_extracts_results = apollo.query(
            {
                "query": sod_extracts,
                "params": {"process_date": args.process_date},
                "results": True,
            }
        )
    except Exception as exc:
        print(f"Failed to fetch SOD extracts: {exc}")
        sod_extracts_results = []

    if args.flag_001:
        # query for EXT001
        try:
            sod_001 = read_sql(SQL_DIR / "sod_extract_001.sql")
            sod_001_results = apollo.query(
                {
                    "query": sod_001,
                    "params": {"process_date": nextday.strftime("%Y-%m-%d")},
                    "results": True,
                }
            )
            if sod_001_results:
                sod_extracts_results.extend(sod_001_results)
        except Exception as exc:
            print(f"Failed to fetch EXT{EXT_001} data: {exc}")

    try:
        existing_query = (
            "SELECT extract FROM batch.sod_extract_runs WHERE process_date=%(process_date)s"
        )
        extracts_already_in_postgres = [
            i[0]
            for i in batch.query(
                {
                    "query": existing_query,
                    "params": {"process_date": args.process_date},
                    "results": True,
                }
            )
        ]
    except Exception as exc:
        print(f"Failed to check existing extracts: {exc}")
        extracts_already_in_postgres = []

    for r in sod_extracts_results:
        if r[1] not in extracts_already_in_postgres:
            process_date_val = (
                nextday.strftime("%Y-%m-%d %H:%M:%S") if r[1] == EXT_001 else args.process_date
            )
            params = {
                "process_date": process_date_val,
                "extract": f"EXT{r[1]}",
                "end_time": r[0].strftime("%Y-%m-%d %H:%M:%S"),
            }
            try:
                batch.query(
                    {
                        "query": (
                            "INSERT INTO batch.sod_extract_runs "
                            "(process_date,extract,end_time) VALUES "
                            "(%(process_date)s,%(extract)s,%(end_time)s)"
                        ),
                        "params": params,
                        "results": False,
                    }
                )
            except Exception as exc:
                print(f"Failed to insert extract {r[1]}: {exc}")


if __name__ == "__main__":
    main()

