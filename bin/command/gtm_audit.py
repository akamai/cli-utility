from __future__ import annotations

import json
import platform
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from akamai_api.gtm import GtmWrapper
from akamai_api.identity_access import IdentityAccessManagement
from rich import print_json
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


def audit(args):

    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    filepath = f'output/{account}_gtm.xlsx' if args.output is None else f'output/{args.output}'

    gtm = GtmWrapper(account_switch_key=args.account_switch_key)

    _, resp = gtm.list_domains()
    master_df = pd.DataFrame()
    combine_df = []
    sheet = {}

    for i, x in enumerate(resp['items']):
        domain = resp['items'][i]['name']
        _, resp_domain = gtm.get_domain(domain)
        try:
            property = resp_domain['properties']
        except:
            logger.error(f'{i} {domain}')
            print_json(data=resp_domain)

        for ind, p in enumerate(property):
            prop_dict = property[ind]['trafficTargets']
            property_name = property[ind]['name']
            df = pd.DataFrame(prop_dict)
            df['Domain'] = domain
            df['Properties'] = property_name
            try:
                df['Type'] = property[ind]['type']
            except:
                logger.error(f'{i} {domain} {property_name}')
                print_json(data=property)
            df['servers'] = df['servers'].str[0]
            combine_df.append(df)

    master_df = pd.concat(combine_df)
    sort_col_df = master_df[['Domain', 'Properties', 'Type', 'enabled',
                             'weight', 'servers', 'name', 'handoutCName',
                             ]]
    master_df = sort_col_df.sort_values(by=['Domain', 'Properties', 'weight', 'enabled'],
                                        ascending=[True, True, False, False],
                                        )

    # cumulative counts of group by object
    cc = master_df.groupby(['Domain', 'Properties', 'Type']).cumcount() + 1
    flat_df = master_df.set_index(['Domain', 'Properties', 'Type', cc]
                                  ).unstack().sort_index(axis='columns', level=1)
    flat_df.columns = ['_'.join(map(str, i)) for i in flat_df.columns]
    flat_df.reset_index(inplace=True)

    sheet['flat_table'] = flat_df
    sheet['raw_data'] = master_df

    if sheet:
        files.write_xlsx(filepath, sheet)
        if platform.system() == 'Darwin' and args.no_show is False:
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
        else:
            logger.info('--no-show argument is supported only on Mac OS')
