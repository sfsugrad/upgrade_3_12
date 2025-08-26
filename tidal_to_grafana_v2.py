import sys

import argparse

from sql_console.sql_console import SqlWrapper

parser = argparse.ArgumentParser()

parser.add_argument(

    '--process-date',

    dest='process_date',

    type=str,

    default=None,

    help='YYYY-MM-DD')

parser.add_argument(

    '--environment',

    dest='environment',

    type=str,

    default=None,

    help='Environment: [dev][uat][prd]')

parser.add_argument(

    '--postgres-username',

    dest='username',

    type=str,

    default=None,

    help='Postgres username')

parser.add_argument(

    '--postgres-password',

    dest='password',

    type=str,

    default=None,

    help='Postgres password')

parser.add_argument(

    '--query',

    dest='query',

    type=str,

    default=None,

    help='Filename that contains relevant query')

parser.add_argument(

    '--origin',

    dest='origin',

    type=str,

    default=None,

    help='Server where data originates')

parser.add_argument(

    '--query-parameters',

    dest='query_parameters',

    type=str,

    default=None,

    help='CSV list of parameters to replace placeholders in specified query')

args = parser.parse_args()

if args.origin == 'ozark':

    connection = SqlWrapper(
        {'env': args.environment, 'method': 'pyodbc', 'server': 'ozark', 'db': 'admiral', 'debug': True,
         'format': 'json'})

elif args.orgin == 'eagle':

    connection = SqlWrapper(
        {'env': args.environment, 'method': 'pyodbc', 'server': 'eagle', 'db': 'tradeking', 'debug': True,
         'format': 'json'})

elif args.orgin == 'hood':

    connection = SqlWrapper(
        {'env': args.environment, 'method': 'pyodbc', 'server': 'hood', 'db': 'fbidb', 'debug': True, 'format': 'json'})

elif args.orgin == 'apollo':

    connection = SqlWrapper(
        {'env': args.environment, 'method': 'pyodbc', 'server': 'apollo', 'db': 'worldwide', 'debug': True,
         'format': 'json'})

else:

    print('Rory is dum')

    sys.exit(1)

batch = SqlWrapper({'env': args.environment, 'method': 'psycopg2', 'server': 'pg' + args.environment, 'db': 'batch',
                    'credentials': {'user': args.username, 'password': args.password}, 'debug': True, 'format': 'json'})

# parse query parameters into correct format

if args.query_parameters is not None:
    args.query_parameters = args.query_parameters.split(',')

with open('sql/' + args.query, 'rt') as f:
    tidal_script = str(f.read()).replace('\n', ' ')

if '[[PROCESSDATE]]' in tidal_script:
    tidal_script = tidal_script.replace('[[PROCESSDATE]]', '\'' + args.process_date + '\'')

if args.query_parameters is not None:

    for index, p in enumerate(args.query_parameters):

        if '[[' + str(index) + ']]' in tidal_script:
            tidal_script = tidal_script.replace('[[' + str(index) + ']]', '\'' + str(p) + '\'')

print('tidal_to_grafana: info: tidal query: ' + tidal_script)

tidal_source_results = connection.query({'query': tidal_script, 'results': True})

tidal_source_results = [i[0] for i in tidal_source_results]  # tuple to list

if len(tidal_source_results) > 0:

    print('tidal source results:')

    for sr in tidal_source_results:

        if sr is not None:

            if sr is not False:

                print(str(sr))

                dest_results = batch.query({'query': str(sr), 'results': True})

                # dest_results = True

                if dest_results is False:

                    print('tidal_to_grafana: error: destination query failed: ' + str(sr))

                    # sys.exit(1)

                else:

                    continue

            else:

                sys.exit(1)

        else:

            continue

else:

    print('tidal_to_grafana: error: script returned no INSERT records...')

    sys.exit(1)