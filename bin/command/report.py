from __future__ import annotations

import os
import platform
import re
import subprocess
import sys
import urllib
from pathlib import Path

import pandas as pd
from akamai_api.identity_access import IdentityAccessManagement
from akamai_api.papi import Papi
from akamai_api.reporting import Reporting
from akamai_utils import cpcode as cp
from akamai_utils import reporting
from pandarallel import pandarallel
from rich import print_json
from rich.console import Console
from rich.table import Table
from tabulate import tabulate
from utils import files


def all_reports(args, logger):
    rpt = Reporting(account_switch_key=args.account_switch_key, logger=logger)
    data = rpt.list_report()
    df = pd.json_normalize(data)

    if not df.empty:
        columns = df.columns.values.tolist()
        columns.remove('description')
        columns.remove('metrics')
        df = df.sort_values(by=['businessObjectName', 'deprecated', 'timeBased', 'name'])
        df['endpoint'] = df['links'].apply(lambda x: reporting.get_execute_report_href(x))
        df = df.query('deprecated == False').copy()

        columns = ['name', 'businessObjectName', 'dataRetentionDays', 'limit', 'maxLimit', 'timeBased', 'endpoint']
        if args.type:
            df = df.query(f"businessObjectName == '{args.type}'").copy()
            if args.namecontains:
                df = df[df['name'].str.contains(args.namecontains)].copy()
            df = df.fillna('')
            df = df.sort_values(by='name')
            df = df.reset_index(drop=True)
            limit = df.limit.unique()
            maxLimit = df.maxLimit.unique()
            console_columns = ['name', 'dataRetentionDays', 'limit', 'maxLimit', 'timeBased', 'endpoint', 'version']
            if limit == maxLimit and limit == ['']:
                console_columns.remove('limit')
                console_columns.remove('maxLimit')
            print()
            print(tabulate(df[console_columns], headers=console_columns, tablefmt='github', showindex='false'))
            print()

        sheet = {}
        sheet['report'] = df
        filepath = 'output/reports_type.xlsx'
        files.write_xlsx(filepath, sheet, freeze_column=1) if not df.empty else None
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath]) if platform.system() == 'Darwin' else None


def offload_by_hostname(args, logger):

    rpt = Reporting(account_switch_key=args.account_switch_key, logger=logger)
    papi = Papi(account_switch_key=args.account_switch_key, logger=logger)
    start, end = reporting.get_start_end(args.interval, int(args.last), logger=logger)
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
    filepath = 'output/reporting_offload_by_host.xlsx'
    files.write_xlsx(filepath, sheets)
    subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath]) if platform.system() == 'Darwin' else None


def offload_by_url(args, logger):

    rpt = Reporting(account_switch_key=args.account_switch_key, logger=logger)
    start, end = reporting.get_start_end(args.interval, int(args.last), logger=logger)
    cpcode_list = args.cpcode
    logger.info(f'Report CpCodes are {" ".join(cpcode_list)}')
    status, data = rpt.hits_by_url(start, end, cpcode_list)

    if not status == 200:
        sys.exit()

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


def traffic_by_response_class(args, logger):
    rpt = Reporting(args.output, account_switch_key=args.account_switch_key, logger=logger)
    iam = IdentityAccessManagement(args.account_switch_key, logger=logger)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    print()
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    account_folder = f'output/reports/{account}'
    Path(account_folder).mkdir(parents=True, exist_ok=True)

    if args.file and args.cpcode:
        sys.exit(logger.error('Please use either --file or --cpcode, not both'))

    start, end = reporting.get_start_end(args.interval, int(args.last), logger=logger)
    # use parallel_apply
    concurrency = int(args.concurrency)
    pandarallel.initialize(progress_bar=True, nb_workers=concurrency, verbose=0)

    if args.cpcode:
        df = pd.DataFrame({'cpcode': args.cpcode})
        df['api'] = df['cpcode'].parallel_apply(lambda x: rpt.traffic_by_response_class(start, end, args.interval, x))
        original_cpcode = df['cpcode'].unique()
    elif args.file:
        sample = int(args.sample) if args.sample else None
        df = pd.read_csv(args.file, header=1, names=['cpcode'], dtype={'cpcode': str}, nrows=sample)
        df = df.drop_duplicates().copy()
        original_cpcode = df['cpcode'].unique()
        df = df.sort_values(by='cpcode')
        df = df.reset_index(drop=True)
        logger.warning(f'\n{df}')
        df['api'] = df['cpcode'].parallel_apply(lambda x: rpt.traffic_by_response_class(start, end, args.interval, x))
    else:
        cpcodes = rpt.traffic_by_response_class(start, end, args.interval)['metadata']['objectIds']
        original_cpcode = list(set(cpcodes))
        logger.info(f'Found {len(cpcodes):,} cpcodes')
        df = pd.DataFrame({'cpcode': cpcodes})
        if args.sample:
            df = df.head(int(args.sample))
        df['api'] = df['cpcode'].parallel_apply(lambda x: rpt.traffic_by_response_class(start, end, args.interval, x))

    sheet = {}
    all_df = df.copy()
    all_df = all_df.reset_index(drop=True)

    # layout 1
    # metrics = ['edgeHits', 'edgeHitsPercent', 'originHits', 'originHitsPercent']
    metrics = ['edgeHitsPercent', 'originHitsPercent']
    exploded_df = all_df.explode('api', ignore_index=True)
    for metric in metrics:
        exploded_df[metric] = exploded_df['api'].apply(lambda x: x.get(metric) if isinstance(x, dict) else None)

    exploded_df[metrics] = exploded_df[metrics].apply(pd.to_numeric, errors='coerce').fillna(0)
    exploded_df['response_class'] = exploded_df['api'].apply(lambda x: x.get('response_class') if isinstance(x, dict) else None)
    logger.debug(exploded_df.shape)
    valid_response_classes = exploded_df['response_class'].dropna().unique()
    filtered_df = exploded_df[exploded_df['response_class'].isin(valid_response_classes)]

    pivot_df = filtered_df.pivot_table(index='cpcode', columns='response_class', values=metrics, fill_value=0)
    pivot_df.columns = [f'{metric}_{col}' for metric, col in pivot_df.columns]
    pivot_df.reset_index(inplace=True)

    print()
    logger.warning('Collecting cpcode name')
    logger.debug(f'\n{pivot_df}')
    pandarallel.initialize(progress_bar=False, verbose=0)
    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key)
    pivot_df['cpcode_name'] = pivot_df['cpcode'].parallel_apply(lambda x: cpc.get_cpcode_name(x))

    '''
    # layout 2
    normalized_df = pd.json_normalize(exploded_df['api'])
    normalized_df[metric] = normalized_df[metric].apply(pd.to_numeric, errors='coerce').fillna(0)
    normalized_df['cpcode'] = exploded_df['cpcode']
    normalized_df = normalized_df.sort_values(by=['cpcode', 'response_class'])
    normalized_df = normalized_df.reset_index(drop=True)
    columns = ['cpcode', 'response_class', 'edgeHitsPercent', 'originHitsPercent']
    logger.warning(normalized_df.shape)
    sheet['report'] = normalized_df[columns]
    '''
    final_cpcode = pivot_df['cpcode'].unique()
    diff = list(set(original_cpcode) - set(final_cpcode))
    if len(diff) > 0:
        diff_df = pd.DataFrame(diff, columns=['cpcode'])
        diff_df = diff_df.sort_values(by='cpcode')
        diff_df = diff_df.reset_index(drop=True)
        diff_df['cpcode_name'] = diff_df['cpcode'].parallel_apply(lambda x: cpc.get_cpcode_name(x))

    columns = list(pivot_df.columns)
    columns.remove('cpcode')
    columns.remove('cpcode_name')
    columns.insert(0, 'cpcode_name')
    columns.insert(0, 'cpcode')

    sheet['report'] = pivot_df[columns]
    if len(diff) > 0:
        sheet['cpcode_with_no_data'] = diff_df
        sheet['original'] = all_df

    filepath = f'{account_folder}/response_class.xlsx'
    files.write_xlsx(filepath, sheet, freeze_column=2) if not pivot_df.empty else None
    files.open_excel_application(filepath, show=True, df=pivot_df)
