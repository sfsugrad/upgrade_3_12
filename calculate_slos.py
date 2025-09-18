import sys

import argparse

import datetime

import calendar

from sql_console.sql_console import SqlWrapper

parser = argparse.ArgumentParser(

    description='')

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

    help='environment [dev][uat][prd]')

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

args = parser.parse_args()

apollo = SqlWrapper({'env': args.environment, 'method': 'pyodbc', 'server': 'apollo', 'db': 'worldwide', 'debug': True,
                     'format': 'json'})

batch = SqlWrapper({'env': args.environment, 'method': 'psycopg2', 'server': 'pg' + args.environment, 'db': 'batch',
                    'credentials': {'user': args.username, 'password': args.password}, 'debug': True, 'format': 'json'})

# calculate day of the week

process_date = datetime.datetime.strptime(args.process_date, '%Y-%m-%d')

slo_date = datetime.datetime.strptime(args.process_date, '%Y-%m-%d') + datetime.timedelta(days=1)

dow_int = process_date.weekday()

dow_english = calendar.day_name[process_date.weekday()].lower()

ConstantName = None

# is it monthly options expiration? or just a regular friday?

# The Scott Method

if dow_int == 4:

    if int(args.process_date[8:10]) >= 15 and int(args.process_date[8:10]) <= 21:

        ConstantName = 'monthly_oe'

    else:

        ConstantName = 'friday'



# must be one of the other SLOs

else:

    if dow_int in [0, 1, 2, 3]:
        ConstantName = 'monday-thursday'

if ConstantName is not None:

    slos = apollo.query({
                            'query': 'select * from ConstantValueLookup where ApplicationName=\'batch_slo\' and ConstantName=\'' + ConstantName + '\'',
                            'results': True})

    for r in slos:

        # get SLO time from ConstantValueLookup record and combine with process date from script input

        slo = datetime.datetime.strftime(slo_date, '%Y-%m-%d') + ' ' + r[3]

        result = batch.query({
                                 'query': 'INSERT INTO batch.slos (process_date, slo) VALUES (\'' + args.process_date + '\', \'' + slo + '\')',
                                 'results': False})

        if result is True:

            sys.exit(0)

        else:

            print('calculate_slos.py: error: INSERT query failed with state: ' + str(result))

            sys.exit(1)

else:

    print(
        'calculate_slos.py: error: either the process_date is on a weekend, or something is very wrong with calculating ConstantName... input arguments: ' + str(
            args))

    sys.exit(1)

apollo.close()

batch.close()

sys.exit(0)