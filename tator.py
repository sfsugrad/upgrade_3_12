"""Execute a parameterized SQL query pipeline.

This script was historically used to pull a query from one of several SQL
servers and execute resulting statements against a Postgres batch database.
The original implementation relied on attribute typos (``args.orgin``) which
raised ``AttributeError`` in Python 3.12.  The modernised version keeps the
behaviour but is compatible with Python 3.12.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

from sql_console.sql_console import SqlWrapper


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--process-date',
        dest='process_date',
        type=str,
        default=None,
        help='YYYY-MM-DD',
    )
    parser.add_argument(
        '--environment',
        dest='environment',
        type=str,
        default=None,
        help='Environment: [dev][uat][prd]',
    )
    parser.add_argument(
        '--postgres-username',
        dest='username',
        type=str,
        default=None,
        help='Postgres username',
    )
    parser.add_argument(
        '--postgres-password',
        dest='password',
        type=str,
        default=None,
        help='Postgres password',
    )
    parser.add_argument(
        '--query',
        dest='query',
        type=str,
        default=None,
        help='Filename that contains relevant query',
    )
    parser.add_argument(
        '--origin',
        dest='origin',
        type=str,
        default=None,
        help='Server where data originates',
    )
    parser.add_argument(
        '--query-parameters',
        dest='query_parameters',
        type=str,
        default=None,
        help='CSV list of parameters to replace placeholders in specified query',
    )

    return parser.parse_args(argv)


def build_source_connection(args: argparse.Namespace) -> SqlWrapper:
    server_config = {
        'ozark': {'server': 'ozark', 'db': 'admiral'},
        'eagle': {'server': 'eagle', 'db': 'tradeking'},
        'hood': {'server': 'hood', 'db': 'fbidb'},
        'apollo': {'server': 'apollo', 'db': 'worldwide'},
    }

    if args.origin not in server_config:
        print('tator: error: origin must be one of: ozark, eagle, hood, apollo')
        sys.exit(1)

    config = server_config[args.origin]
    return SqlWrapper(
        {
            'env': args.environment,
            'method': 'pyodbc',
            'server': config['server'],
            'db': config['db'],
            'debug': True,
            'format': 'json',
        }
    )


def build_batch_connection(args: argparse.Namespace) -> SqlWrapper:
    return SqlWrapper(
        {
            'env': args.environment,
            'method': 'psycopg2',
            'server': 'pg' + args.environment,
            'db': 'batch',
            'credentials': {'user': args.username, 'password': args.password},
            'debug': True,
            'format': 'json',
        }
    )


def load_query(query_filename: str) -> str:
    query_path = Path('sql') / query_filename
    with query_path.open('rt', encoding='utf-8') as f:
        return f.read().replace('\n', ' ')


def apply_parameters(script: str, args: argparse.Namespace) -> str:
    tidal_script = script

    if '[[PROCESSDATE]]' in tidal_script and args.process_date:
        tidal_script = tidal_script.replace('[[PROCESSDATE]]', f"'{args.process_date}'")

    if args.query_parameters:
        parameters = [param.strip() for param in args.query_parameters.split(',') if param.strip()]
        for index, param in enumerate(parameters):
            placeholder = f'[[{index}]]'
            if placeholder in tidal_script:
                tidal_script = tidal_script.replace(placeholder, f"'{param}'")

    return tidal_script


def run(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)

    connection = build_source_connection(args)
    batch = build_batch_connection(args)

    tidal_script = apply_parameters(load_query(args.query), args)
    print('tidal_to_grafana: info: tidal query: ' + tidal_script)

    tidal_source_results = connection.query({'query': tidal_script, 'results': True})
    tidal_source_results = [i[0] for i in tidal_source_results]  # tuple to list

    if tidal_source_results:
        print('tidal source results:')
        for sr in tidal_source_results:
            if sr:
                print(str(sr))
                dest_results = batch.query({'query': str(sr), 'results': True})
                if dest_results is False:
                    print('tidal_to_grafana: error: destination query failed: ' + str(sr))
    else:
        print('tidal_to_grafana: error: script returned no INSERT records...')
        sys.exit(1)


if __name__ == '__main__':
    run()
