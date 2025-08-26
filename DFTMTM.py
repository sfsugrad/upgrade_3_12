import sys

import pyodbc

import argparse

import re

import codecs

import string

import os

from datetime import datetime, timedelta

from tzlocal import get_localzone  # pip install tzlocal

# folders = ['DTFMTA','DTFMTD','DTFMTE','DTFMTH','DTFMTI','DTFMTM','DTFPDQ']

folders = ['DTFMTM']

DAY = timedelta(1)

infile = ''

out_file = ''

office = []

officecode = []

lines = []

files = []

patterns = []

first_row = []

DAY = timedelta(1)

local_tz = get_localzone()  # get local timezone

now = datetime.now(local_tz)  # get timezone-aware datetime object

day_ago = local_tz.normalize(now - DAY)  # exactly 24 hours ago, time may differ

naive = now.replace(tzinfo=None) - DAY  # same time

yesterday = local_tz.localize(naive, is_dst=None)  # but elapsed hours may differ

yesterday_date = yesterday.strftime("%m%d")

yesterday_date_full = yesterday.strftime("%Y%m%d")

today = now.strftime("%Y%m%d")


def parse_stuff(infile, folder, outfile):
    # outfile = open(r'\\d1wrptfsrprd3\reports\Firm10\MTGO\\' + yesterday_date_full + '\\' + folder + '.txt','w')

    # print(outfile + 'MTGO\\' + today)

    if not os.path.exists(outfile + 'MTGO\\' + today):
        os.mkdir(outfile + 'MTGO\\' + today)

    out_file = open(outfile + 'MTGO\\' + today + '\\' + folder + '.txt', 'w')

    # r'\\d1wrptfsrprd3\reports\Firm10\MTGO\

    aline = infile.readline()

    while aline:

        for pattern in patterns:

            m = re.search(pattern, aline)

            if m:

                out_file.write(aline)

                aline = infile.readline()

            else:

                aline = infile.readline()

    infile.close()

    out_file.close()


def main(argv=None):
    if argv is None:
        argv = sys.argv

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("SourceFilePath")

    arg_parser.add_argument("Outfile")

    arg_parser.add_argument("Server")

    arg_parser.add_argument("Database")

    args = arg_parser.parse_args()

    filepath = args.SourceFilePath

    # print(filepath)

    out_file = args.Outfile

    # print(out_file)

    # dbserver = 'D1WLUOSQLNP1\dev'

    # dbserver = 'APOLLO\HISTORY'

    dbserver = args.Server

    # db = 'Process10'

    # db = 'Worldwide'

    db = args.Database

    con_str = 'DRIVER={SQL Server Native Client 10.0};' + 'SERVER={};DATABASE={};Trusted_Connection=yes'.format(
        dbserver, db)

    cnxn = pyodbc.connect(con_str)

    cursor = cnxn.cursor()

    select_officecode = "select distinct officecode from correspondentoffice where correspondentcode = 'MTGO' and len(officecode) = 3"

    cursor.execute(select_officecode)

    first_row = cursor.fetchall()

    for x in first_row:
        office.append(x)

    for i in office:
        s = '|'.join(i)

        patterns.append(s)

    for folder in folders:

        # print(folder)

        try:

            # infile = open(r'\\d1wappsvrprd1\apps\DTC\DTFPART' + '\\' + folder + '\\' + folder + '_OUTPUT_' + yesterday_date + '.txt','rb')

            # infile = open(r'\\d1wrptfsrnp1\dev\APPS\DTC\DTFPART' + '\\' + folder + '\\' + folder + '_OUTPUT_' + yesterday_date + '.txt','rb')

            infile = open(
                filepath + 'DTC\DTFPART' + '\\' + folder + '\\' + folder + '_OUTPUT_' + yesterday_date + '.txt', 'rb')

            # print(infile)

            parse_stuff(infile, folder, out_file)

        except IOError as (errno, strerror):

            print
            "I/O error({0}): {1}".format(errno, strerror)


if __name__ == '__main__':
    main()

# infile.close()

# outfile.close()