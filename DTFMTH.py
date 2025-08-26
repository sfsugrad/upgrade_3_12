import sys
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path


# folders = ['DTFMTA','DTFMTD','DTFMTE','DTFMTH','DTFMTI','DTFMTM','DTFPDQ']
folders = ['DTFMTH']

DAY = timedelta(1)

office = []
officecode = []
lines = []
files = []
patterns = []
first_row = []

# Date placeholders; populated during runtime
yesterday_date = ""
yesterday_date_full = ""
today = ""


def parse_stuff(infile, folder: str, outdir: str) -> None:
    """Filter lines from ``infile`` using ``patterns`` and write them to ``outdir``.

    Parameters
    ----------
    infile : io.TextIOBase
        Input file to scan.
    folder : str
        Name of the folder being processed.
    outdir : str
        Root directory for output files.
    """

    output_dir = Path(outdir) / "MTGO" / today
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{folder}.txt"

    with out_path.open("w", encoding="utf-8") as out_file:
        for line in infile:
            for pattern in patterns:
                if re.search(pattern, line):
                    out_file.write(line)
                    break


def main(argv=None):
    if argv is None:
        argv = sys.argv

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("SourceFilePath")
    arg_parser.add_argument("Outfile")
    arg_parser.add_argument("Server")
    arg_parser.add_argument("Database")
    args = arg_parser.parse_args()

    # Import dependencies lazily so that help text can be displayed without them
    from tzlocal import get_localzone  # pip install tzlocal
    import pyodbc

    filepath = args.SourceFilePath
    out_file = args.Outfile
    dbserver = args.Server
    db = args.Database

    # Compute date values
    global today, yesterday_date, yesterday_date_full
    local_tz = get_localzone()  # get local timezone
    now = datetime.now(local_tz)  # get timezone-aware datetime object
    day_ago = local_tz.normalize(now - DAY)  # exactly 24 hours ago, time may differ
    naive = now.replace(tzinfo=None) - DAY  # same time
    yesterday = local_tz.localize(naive, is_dst=None)  # but elapsed hours may differ
    yesterday_date = yesterday.strftime("%m%d")
    yesterday_date_full = yesterday.strftime("%Y%m%d")
    today = now.strftime("%Y%m%d")

    con_str = (
        "DRIVER={SQL Server Native Client 10.0};"
        "SERVER={};DATABASE={};Trusted_Connection=yes".format(dbserver, db)
    )

    cnxn = pyodbc.connect(con_str)
    cursor = cnxn.cursor()
    select_officecode = (
        "select distinct officecode from correspondentoffice "
        "where correspondentcode = 'MTGO' and len(officecode) = 3"
    )
    cursor.execute(select_officecode)
    first_row = cursor.fetchall()
    for x in first_row:
        office.append(x)
    for i in office:
        s = "|".join(i)
        patterns.append(s)
    for folder in folders:
        try:
            infile_path = (
                Path(filepath)
                / "DTC"
                / "DTFPART"
                / folder
                / f"{folder}_OUTPUT_{yesterday_date}.txt"
            )
            with infile_path.open("r", encoding="utf-8") as infile:
                parse_stuff(infile, folder, out_file)
        except OSError as e:
            print(f"I/O error({e.errno}): {e.strerror}")


if __name__ == "__main__":
    main()

