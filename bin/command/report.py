from __future__ import annotations

import json
import os
import urllib

import pandas as pd
from akamai_api.papi import Papi
from akamai_api.reporting import Reporting
from akamai_utils import reporting
from pandarallel import pandarallel
from rich import print_json
from rich.console import Console
from rich.table import Table
from tabulate import tabulate
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


def offload(args):

    rpt = Reporting(account_switch_key=args.account_switch_key)
    papi = Papi(account_switch_key=args.account_switch_key)
    start, end = reporting.get_start_end()
    data = rpt.hits_by_hostname(start, end)

    df = pd.DataFrame(data)
    df['edgeHits'] = df['edgeHits'].astype(int)

    logger.warning('Looking property by hostname ...')
    pandarallel.initialize(progress_bar=True)
    df['property_name'] = df['hostname'].parallel_apply(lambda x: papi.search_property_by_hostname(x))
    stat_df = df.groupby(['property_name']).sum()
    flat_data = stat_df.reset_index()
    stat_df = flat_data[['property_name', 'edgeHits']]
    stat_df = stat_df.sort_values(by='edgeHits', ascending=False)

    sheets = {}
    sheets['hostname_hit'] = df
    sheets['hit_by_property'] = stat_df
    top_five = stat_df.head(5)
    logger.warning('Top 5 hits, for a full list please check excel file')
    print(tabulate(top_five, headers=['hostname', 'edgeHits'], tablefmt='simple', showindex='false'))
    print()
    files.write_xlsx('output/reporting_offload_by_host.xlsx', sheets)


def url_offload(args):

    rpt = Reporting(account_switch_key=args.account_switch_key)
    papi = Papi(account_switch_key=args.account_switch_key)
    start, end = reporting.get_start_end()
    cpcode_list = args.cpcodes
    logger.info(f'Report CpCodes are {" ".join(cpcode_list)}')
    data = rpt.hits_by_url(start, end, cpcode_list)

    # extract file extension from urls
    for url in data:
        url_filename, url['extension'] = os.path.splitext(url['hostname.url'])
        full_url = f"https://{url['hostname.url']}"
        url['hostname'] = urllib.parse.urlparse(full_url).hostname
        url['path'] = urllib.parse.urlparse(full_url).path

    df = pd.DataFrame(data)
    df['allEdgeHits'] = df['allEdgeHits'].astype(int)
    df['allOriginHits'] = df['allOriginHits'].astype(int)
    csv_data = df.to_csv()

    # create seperate dataframe to group by file extension
    df_ext = df[['extension', 'allEdgeHits', 'allOriginHits']]
    df_ext = df_ext.groupby('extension').sum()
    df_ext['offload'] = (df_ext['allEdgeHits'] - df_ext['allOriginHits']) / (df_ext['allEdgeHits'])
    df_ext.reset_index(inplace=True)
    ext_dict_data = df_ext.to_dict('records')

    table = Table(title='File Extension Offload')
    table.add_column('extension', style='cyan')
    table.add_column('edge_hits', style='magenta')
    table.add_column('origin_hits', style='magenta')
    table.add_column('offload', style='green')

    for row in ext_dict_data:
        row['offload'] = str('{:.2f}%'.format(row['offload']))

        table.add_row(str(row['extension']), str(row['allEdgeHits']), str(row['allOriginHits']), row['offload'])

    console = Console()
    console.print(table)


def all_reports(args):
    rpt = Reporting(account_switch_key=args.account_switch_key)
    data = rpt.list_report()
    df = pd.DataFrame(data)
    logger.debug(df)
    sheets = {}
    sheets['report'] = df
    files.write_xlsx('output/reporting.xlsx', sheets)
