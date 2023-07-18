from __future__ import annotations

import re
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd
from akamai_api.cps import CpsWrapper
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
from ipwhois import IPWhois
from pandarallel import pandarallel
from pytz import utc
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import files
from utils import google_dns as gg

logger = lg.setup_logger()


def audit(args):

    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    filepath = f'output/{account}_certificate.xlsx' if args.output is None else f'output/{args.output}'

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
    contract_data = []
    contract_host = []
    pandarallel.initialize(progress_bar=False, verbose=0)

    for contract_id in contracts:
        print()
        msg = f'Collect certificate for {contract_id=}'
        _, df = cps.list_enrollments(contract_id, enrollment_ids=csv_list)

        filtered = False
        if df.empty:
            logger.warning(f'{msg} - found no certificate')
        else:
            logger.warning(f'{msg}')
            columns = ['contractId', 'id', 'Slot', 'sni', 'ra', 'common_name', 'hostname_count']
            # print(tabulate(df[columns], headers=columns))

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
                df = df[df['Slot'].isin(args.slot)].copy()
                df = df.reset_index(drop=True)

        if not df.empty:
            if filtered is True:
                logger.warning('Filter based on selected criteria')
                columns = ['contractId', 'id', 'Slot', 'sni', 'ra', 'common_name', 'hostname_count']
                print(tabulate(df[columns], headers=columns))

            hostname_df = pd.DataFrame
            hostname_df = df.query('hostname_count > 0')
            hostname_df = hostname_df.explode('hostname')
            if not hostname_df.empty:
                del hostname_df['hostname_count']
                hostname_df['Slot'] = hostname_df['Slot'].astype(str)
                total = sum(df['hostname_count'])

                logger.warning(f'Collecting CNAME for all {total:,} hosts, please be patient')
                hostname_df['cname'] = hostname_df['hostname'].parallel_apply(lambda x: gg.dnslookup(x) if x is not None else '')

                logger.warning('Determine if CNAMEd to Akamai')
                hostname_df['cname_to_akamai'] = hostname_df['cname'].parallel_apply(lambda x: 'True' if 'edgekey' in x or 'edgesuite' in x else '')

                logger.warning('Determine if IP belongs to Akamai')
                hostname_df['valid_ip'] = hostname_df.parallel_apply(lambda row: is_valid_ip(row['cname']), axis=1)
                hostname_df['ASN_Description'] = hostname_df.parallel_apply(lambda row: asn(row['cname']) if row['valid_ip'] is True else None, axis=1)
                contract_host.append(hostname_df)

            df['hostname_one_per_line'] = df['hostname'].parallel_apply(lambda x: '\n'.join(''.join(c) for c in x))
            df['hostname_with_ending_comma'] = df['hostname'].parallel_apply(lambda x: ',\n'.join(''.join(c) for c in x))
            del df['hostname']

            columns = ['contractId', 'id', 'Slot', 'sni', 'ra', 'common_name', 'expiration_date',
                       'hostname_count', 'hostname_one_per_line', 'hostname_with_ending_comma']
            df['expiration_date'] = df['id'].parallel_apply(lambda x: cps.certificate_expiration_date(x))
            if args.expire is True:
                filtered = True
                filtered_df = df.copy()
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=utc)
                filtered_df['expiration_date'] = pd.to_datetime(df['expiration_date'])
                filtered_df = filtered_df.query('expiration_date < @today')
                filtered_df = filtered_df.reset_index(drop=True)
                filtered_df['expiration_date'] = filtered_df['expiration_date'].apply(
                    lambda x: x.replace(hour=23, minute=59, second=59).strftime('%Y-%m-%dT%H:%M:%SZ'))
                temp_col = ['contractId', 'id', 'Slot', 'sni', 'ra', 'common_name', 'expiration_date']
                logger.info(f'\n{filtered_df[temp_col]}')
                if not filtered_df.empty:
                    contract_data.append(filtered_df[columns])
            else:
                contract_data.append(df[columns])

            # sheet[f'summary_{contract_id}'] = df
            # sheet[f'hostname_{contract_id}'] = hostname_df

    if len(contract_data) > 0:
        sheet['summary'] = pd.concat(contract_data)

    if len(contract_host) > 0:
        sheet['hostname'] = pd.concat(contract_host)

        x = pd.concat(contract_host).query('valid_ip')
        if not x.empty:
            y = x.cname.drop_duplicates()
            ip_df = y.to_frame()
            ip_df = ip_df.sort_values(by='cname')
            ip_df = ip_df.reset_index(drop=True)
            logger.debug(f'\n{ip_df}')
            ip_df['ASN_Description'] = ip_df['cname'].parallel_apply(lambda x: asn(x))
            sheet['ip'] = ip_df

    if sheet:
        files.write_xlsx(filepath, sheet)
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath]) if args.show else None


def is_valid_ip(ip: str):
    pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    return re.match(pattern, ip) is not None


def asn(ip):
    max_retries = 2
    retry_delay = 1  # seconds

    for attempt in range(max_retries):
        try:
            whois_info = IPWhois(ip).lookup_rdap()['asn_description']
            logger.debug(f'{ip:<30} {whois_info:<20} {attempt}')
            break
        except Exception as e:
            logger.debug(f'{ip:<30} RDAP lookup {attempt}')
            time.sleep(retry_delay)
    else:
        try:
            whois_info = IPWhois(ip).lookup_whois()['asn_description']
            if whois_info is None:
                whois_info = '_NOT FOUND'
        except Exception as e:
            logger.error(f'{ip:<30} WHOIS lookup {attempt}')
            return '__NOT FOUND'

    return whois_info
