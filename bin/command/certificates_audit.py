from __future__ import annotations

import re
import subprocess
import sys

import pandas as pd
from akamai_api.cps import CpsWrapper
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
from pandarallel import pandarallel
from tabulate import tabulate
from utils import _logging as lg
from utils import files
from utils import google_dns as gg

logger = lg.setup_logger()


def audit(args):

    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)
    try:
        account = re.sub(r'[.,]|_{2}|Direct_Customer|Indirect_Customer|_', '', account)  # shorten account name
        filepath = f'output/certificate_{account}.xlsx'
    except:
        sys.exit(logger.error(account['detail']))

    cps = CpsWrapper(account_switch_key=args.account_switch_key)
    csv_list = []
    if args.enrollment_id and not args.contract_id:
        csv_list = [int(value) for value in args.enrollment_id]

        if len(args.enrollment_id) == 1:
            _, response = cps.certificate_deployment(enrollment_id=int(args.enrollment_id[0]))
            df = pd.DataFrame.from_dict(response, orient='index')
            df = df.sort_index()
            print(tabulate(df, showindex=True))
            sys.exit()

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    contracts = args.contract_id if args.contract_id else papi.get_contracts()
    sheet = {}

    for contract_id in contracts:
        logger.warning(f'Collect certificate for {contract_id=}')
        _, df = cps.list_enrollments(contract_id, enrollment_ids=csv_list)

        filtered = False
        if not df.empty:
            columns = ['contractId', 'id', 'Slot', 'sni', 'ra', 'common_name', 'hostname_count']
            print(tabulate(df[columns], headers=columns))

            if args.sni is True:
                filtered = True
                df = df.query('sni')
                df = df.reset_index(drop=True)
                logger.debug(f'\n{df}')
            if args.authority:
                filtered = True
                df = df[df['ra'].isin(args.authority)].copy()
                df = df.reset_index(drop=True)
                logger.debug(f'\n{df}')
            if args.slot:
                filtered = True
                int_slot = [int(x) for x in args.slot]
                df = df[df['productionSlots'].isin(int_slot)].copy()
                df = df.reset_index(drop=True)
                logger.debug(f'\n{df}')

        if not df.empty:
            if filtered is True:
                logger.warning('Filter based on selected criteria')
                columns = ['contractId', 'id', 'Slot', 'sni', 'ra', 'common_name', 'hostname_count']
                print(tabulate(df[columns], headers=columns))
            hostname_df = df.explode('hostname')
            del hostname_df['hostname_count']
            hostname_df['Slot'] = hostname_df['Slot'].astype(str)
            pandarallel.initialize(progress_bar=False)

            logger.warning('Check CName for each host')
            hostname_df['cname'] = hostname_df['hostname'].parallel_apply(lambda x: gg.dnslookup(x) if x is not None else '')
            hostname_df['cname_to_akamai'] = hostname_df['cname'].apply(lambda x: 'True' if 'edgekey' in x or 'edgesuite' in x else '')

            df['hostname_one_per_line'] = df['hostname'].parallel_apply(lambda x: '\n'.join(''.join(c) for c in x))
            df['hostname_with_ending_comma'] = df['hostname'].parallel_apply(lambda x: ',\n'.join(''.join(c) for c in x))
            del df['hostname']

            df['expiration_date'] = df['id'].parallel_apply(lambda x: cps.certificate_expiration_date(x))
            sheet[f'summary_{contract_id}'] = df
            sheet[f'hostname_{contract_id}'] = hostname_df
            files.write_xlsx(filepath, sheet)
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath]) if args.show else None

            # list_enrollments = cps.collect_enrollments(contract_id, ctr_enrollments, csv_list)
            # enrollments.extend(list_enrollments) if len(list_enrollments) > 0 else None
