from __future__ import annotations

import pandas as pd
from akamai_api.papi import Papi
from akamai_api.reporting import Reporting
from akamai_utils import reporting
from pandarallel import pandarallel
from rich import print_json
from utils import files
from utils._logging import setup_logger


logger = setup_logger()


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
    files.write_xlsx('output/reporting_offload_by_host.xlsx', sheets)


def all_reports(args):
    rpt = Reporting(account_switch_key=args.account_switch_key)
    data = rpt.list_report()
    df = pd.DataFrame(data)
    logger.debug(df)
    sheets = {}
    sheets['report'] = df
    files.write_xlsx('output/reporting.xlsx', sheets)
