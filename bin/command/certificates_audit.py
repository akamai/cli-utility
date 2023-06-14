from __future__ import annotations

import re
import subprocess
import sys

import pandas as pd
from akamai_api.cps import CpsWrapper
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
from tabulate import tabulate
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


def audit(args):
    pd.set_option('display.max_rows', 20)
    pd.set_option('max_colwidth', 50)

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
    enrollments = []
    if args.contract_id:
        logger.debug(f'{sorted(csv_list)=}') if csv_list else None
        enrollments, all_df, df = cps.list_enrollments(contract_id=args.contract_id, enrollment_ids=csv_list)
        if args.sni is True:
            df = df[df['sni'] is True].copy()
        if args.authority is True:
            df = df[df['ra'] == args.authority].copy()
        if args.slot is True:
            df = df[df['slot'].isin(args.slot)].copy()

        print(df)
        list_enrollments = cps.collect_enrollments(args.contract_id, enrollments, csv_list)
        enrollments.extend(list_enrollments)
    else:
        contracts = papi.get_contracts()
        for contract_id in contracts:
            logger.warning(f'Collect certificate for {contract_id=}')
            enrollments, all_df, df = cps.list_enrollments(contract_id, enrollment_ids=csv_list)
            if args.sni is True:
                df = df.query('sni')
                df = df.reset_index(drop=True)
                logger.debug(f'\n{df}')
            if args.authority:
                df = df[df['ra'].isin(args.authority)].copy()
                df = df.reset_index(drop=True)
                logger.debug(f'\n{df}')
            if args.slot:
                int_slot = [int(x) for x in args.slot]
                df = df[df['productionSlots'].isin(int_slot)].copy()
                logger.debug(f'\n{df}')

            filter_df = df
            list_enrollments = cps.collect_enrollments(contract_id, enrollments, csv_list)
            if list_enrollments:
                enrollments.extend(list_enrollments)

    # final result
    df = pd.DataFrame(enrollments)
    df = df.sort_values(by=['expire_date', 'common_name', 'CNAME', 'enrollmentId', 'slot', 'hostname'])
    df['slot'] = df['slot'].astype(str)
    df = df.reset_index(drop=True)
    logger.error(f'\n{df}')
    sheet = {}
    sheet['certificate'] = df
    files.write_xlsx(filepath, sheet)

    if args.show:
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
