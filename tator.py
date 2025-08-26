import argparse

import requests

import sys

import time

from datetime import datetime, timezone

parser = argparse.ArgumentParser(description='add annotations to grafana')

parser.add_argument('--host', metavar='HOST', type=str, required=True, help='grafana host')

parser.add_argument('--token', metavar='TOKEN', type=str, required=True,
                    help='API token to pass in Authorization header')

parser.add_argument('--text', metavar='TEXT', type=str, required=True, help='annotation text')

parser.add_argument('--dashboard', metavar='DASHBOARD_ID', type=int, required=False, default=None,
                    help='dashboard to annotate')

parser.add_argument('--panel', metavar='PANEL_ID', type=int, required=False, default=None, help='panel to annotate')

parser.add_argument('--tags', metavar='[TAG,...]', type=lambda s: [str(i).strip() for i in s.split(',')],
                    required=False, default=None, help='annotation tags')

parser.add_argument('--time', metavar='TIME', type=int, required=False, default=None,
                    help='instantaneous Unix epoch time in milliseconds')

parser.add_argument('--timeStr', metavar='%Y-%m-%dT%H:%M:%S', type=str, required=False, default=None,
                    help='datetime string formatted as %yyyy-%mm-%ddT%hh:%mm:%ss')

args = parser.parse_args()

if __name__ == '__main__':

    headers = {'Authorization': 'Bearer ' + args.token, 'Accept': 'application/json',
               'Content-Type': 'application/json'}

    json_body = {

        "dashboardId": args.dashboard,

        "panelId": args.panel,

        "time": args.time,

        "tags": args.tags,

        "text": args.text

    }

    if args.dashboard is None:
        del json_body['dashboardId']

    if args.panel is None:
        del json_body['panelId']

    if args.tags is None:
        del json_body['tags']

    if args.time is None and args.timeStr is None:

        args.time = int(time.time() * 1000)

    elif args.time is None and args.timeStr is not None:

        args.time = int(
            datetime.strptime(args.timeStr, '%Y-%m-%dT%H:%M:%S')
            .replace(tzinfo=timezone.utc)
            .timestamp() * 1000
        )

    r = requests.post(

        args.host + '/api/annotations',

        headers=headers,

        json=json_body

    )

    if r.status_code != 200:

        print(f"{r.status_code}: {r.text}")

        sys.exit(1)

    else:

        sys.exit(0)
