import argparse

import datetime

from sql_console.sql_console import SqlWrapper

parser = argparse.ArgumentParser(

    description='Gets SOD extract runs from ReportRequest and ships them to Postgres')

parser.add_argument(

    '--environment',

    dest='environment',

    type=str,

    default=None,

    help='Environment: [prod][uat][dev]')

parser.add_argument(

    '--process-date',

    dest='process_date',

    type=str,

    default=None,

    help='YYYY-MM-DD')

parser.add_argument(

    '--postgres-username',

    dest='username',

    type=str,

    default=None,

    help='Username for Postgres')

parser.add_argument(

    '--postgres-password',

    dest='password',

    type=str,

    default=None,

    help='Password for Postgres')

parser.add_argument(

    '--notgucci-argument-001',

    dest='flag_001',

    action='store_true',

    help='Stupid workaround for EXT001')

parser.set_defaults(flag_001=False)

args = parser.parse_args()

# calculate next day from given processdate

nextday = datetime.datetime.strptime(args.process_date, '%Y-%m-%d') + datetime.timedelta(days=1)

apollo = SqlWrapper({'env': args.environment, 'method': 'pyodbc', 'server': 'apollo', 'db': 'worldwide', 'debug': True,
                     'format': 'json'})

batch = SqlWrapper({'env': args.environment, 'method': 'psycopg2', 'server': 'pg' + args.environment, 'db': 'batch',
                    'credentials': {'user': args.username, 'password': args.password}, 'debug': True, 'format': 'json'})

# query for all extracts excluding EXT001

with open('sql/sod_extracts.sql', 'rt') as f:
    sod_extracts = str(f.read()).replace('\n', ' ')

sod_extracts = sod_extracts.replace('[[PROCESSDATE]]', '\'' + args.process_date + '\'')

sod_extracts_results = apollo.query({'query': sod_extracts, 'results': True})

if args.flag_001 is True:

    # query for EXT001

    with open('sql/sod_extract_001.sql', 'rt') as f:

        sod_001 = str(f.read()).replace('\n', ' ')

    sod_001 = sod_001.replace('[[PROCESSDATE]]', '\'' + datetime.datetime.strftime(nextday, '%Y-%m-%d') + '\'')

    sod_001_results = apollo.query({'query': sod_001, 'results': True})

    if len(sod_001_results) > 0:

        for r in sod_001_results:
            sod_extracts_results.append(r)

extracts_already_in_postgres = [i[0] for i in batch.query(
    {'query': 'SELECT extract FROM batch.sod_extract_runs WHERE process_date=\'' + args.process_date + '\'',
     'results': True})]

for r in sod_extracts_results:

    if r[1] not in extracts_already_in_postgres:

        if r[1] == '001':

            batch.query({
                            'query': 'INSERT INTO batch.sod_extract_runs (process_date,extract,end_time) VALUES(\'' + datetime.datetime.strftime(
                                nextday, '%Y-%m-%d %H:%M:%S') + '\', \'EXT' + r[
                                         1] + '\', \'' + datetime.datetime.strftime(r[0], '%Y-%m-%d %H:%M:%S') + '\')',
                            'results': False})

        else:

            batch.query({
                            'query': 'INSERT INTO batch.sod_extract_runs (process_date,extract,end_time) VALUES(\'' + args.process_date + '\', \'EXT' +
                                     r[1] + '\', \'' + datetime.datetime.strftime(r[0], '%Y-%m-%d %H:%M:%S') + '\')',
                            'results': False})