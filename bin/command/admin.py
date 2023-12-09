from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from akamai_api.identity_access import IdentityAccessManagement
from pandarallel import pandarallel
from rich import print_json
from tabulate import tabulate
from utils import files


def homepage_url(account_id: str, contract_id: str) -> str:
    url = 'https://control.akamai.com/apps/home-page/#/manage-account?accountId='
    if account_id:
        url = f'{url}{account_id}&contractTypeId={contract_id}&targetUrl='
        # this clickable link using escape sequences may not work in all terminal emulators or environments
        return f'\033]8;;{url}\033\\{url}\033]8;;\033\\'
    else:
        return ''


def cleanup_arguments(value: str, logger=None):
    if value == '*':
        logger.error(f'not allow:             {value:<20}')
        return value
    elif len(value) < 3:
        logger.error(f'minimum 3 characters:  {value:<20}')
        return value


def remove_and_store_substrings(row):
    """ Function to remove specific substrings and store them in a new column """
    substrings_to_remove = ['_Akamai Internal',
                            '_Indirect Customer', '_Direct Customer',
                            '_Marketplace Prospect',
                            '_NAP Master Agreement',
                            '_NAP Master Agreement ',
                            '_Value Added Reseller',
                            '_Value Added Reseller ',
                            '_Tier 1 Reseller',
                            '_Tier 1 Reseller ',
                            '_VAR Customer',
                            '_ISP']

    removed_substrings = []
    for substring in substrings_to_remove:
        if substring in row['Akamai_Account']:
            removed_substrings.append(substring)
            row['Akamai_Account'] = row['Akamai_Account'].replace(substring, '')
    return pd.Series({'Akamai_Account': row['Akamai_Account'], 'Account_Type': ', '.join(removed_substrings)})


def lookup_account(args, logger=None):
    if args.account and args.input:
        sys.exit(logger.error('Please provide either --account or --input, not both'))

    iam = IdentityAccessManagement(args.account_switch_key, args.section, logger=logger)
    searches = sorted(args.account) if args.account else None

    if args.input:
        with open(args.input) as file:
            searches = [line.rstrip('\n') for line in file.readlines()]

    # cleanup keywords
    print()
    logger.warning(f'Lookup values:           {searches}')
    removed = []
    for value in searches:
        value = cleanup_arguments(str(value))
        removed.append(value) if value else None

    # summary
    final_searches = sorted(list(set(searches) - set(removed)))
    if len(final_searches) == 0:
        sys.exit()
    if not set(searches) == set(final_searches):
        logger.warning(f'Remaining lookup values: {final_searches}')

    # display result
    print()
    results = []
    longest = max(final_searches, key=len)
    length = len(longest)
    all_accounts = []
    for value in final_searches:
        offset = length - len(value)
        PRESERVE_WHITESPACE = ' ' * offset
        index_header = f'{value.upper()}{PRESERVE_WHITESPACE}'
        logger.debug(f'{len(value)} {length} {index_header}]')

        data = iam.search_accounts(value)
        if len(data) > 0:
            results.extend(data)
            df = pd.DataFrame(data)
            df = df.sort_values(by=['accountName', 'accountSwitchKey'], key=lambda col: col.str.lower())
            df.index = np.arange(1, len(df) + 1)
            flatten_df = df.reset_index()
            flatten_df = flatten_df.rename(columns={'index': index_header})
            flatten_df.loc[-1, :] = [None, '', '']
        else:
            flatten_df = pd.DataFrame(columns=[index_header, 'accountSwitchKey', 'accountName', 'url'])
            flatten_df.loc['1', :] = ['', None, 'not found', ' ']

        # replace nan row with blank, remove .0
        flatten_df = flatten_df.replace(np.nan, '', regex=True)
        flatten_df[index_header] = flatten_df[index_header].astype(str).apply(lambda x: x[:-2])
        df = flatten_df

        # extract accountSwitchKey to generate direct url
        df['accountId'] = df['accountSwitchKey'].str.split(':').apply(lambda col: col[0] if len(col) > 0 else col)
        df['contractTypeId'] = df['accountSwitchKey'].str.split(':').apply(lambda col: col[1] if len(col) > 1 else col)
        df['url'] = df.apply(lambda row: homepage_url(row['accountId'], row['contractTypeId']), axis=1)

        # drop header and unwanted columns
        accountSwitchKey = ' ' * 20
        accountName = ' ' * 60
        url = ' ' * 100
        df = df.drop(['accountId', 'contractTypeId'], axis=1)
        columns = [index_header, accountSwitchKey, accountName, url]
        print(tabulate(df, headers=columns, showindex=False, tablefmt='github'))
        all_accounts.append(df[['accountName', 'accountSwitchKey']])

    df = pd.concat(all_accounts)
    df = df.reset_index(drop=True)
    new_column_names = ['Akamai_Account', 'Account_SwitchKey']
    df.columns = new_column_names
    df.index = df.index + 1
    df = df.replace('', np.nan)
    df = df.dropna()

    if df.empty:
        sys.exit()
    else:
        if args.xlsx:
            pandarallel.initialize(progress_bar=False, verbose=0)

            df['accountId'] = df['Account_SwitchKey'].parallel_apply(lambda x: x.split(':')[0])
            df['contractTypeId'] = df['Account_SwitchKey'].parallel_apply(lambda x: x.split(':')[1])
            url = 'https://control.akamai.com/apps/home-page/#/manage-account?'
            df['url_1'] = df.parallel_apply(lambda row: f'{url}accountId={row["accountId"]}&contractTypeId={row["contractTypeId"]}&targetUrl=', axis=1)

            # Apply the function to each row
            columns = ['no.', 'Akamai_Account', 'Account_SwitchKey', 'Account_Type']
            df[['Akamai_Account', 'Account_Type']] = df.parallel_apply(remove_and_store_substrings, axis=1)
            df['Account(Hyperlink)'] = df.parallel_apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['url_1'], row['Akamai_Account']), axis=1)
            df['Account(Hyperlink)'] = df.parallel_apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['url_1'], row['Akamai_Account']), axis=1)
            df['sk'] = df.parallel_apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['url_1'], row['Account_SwitchKey']), axis=1)

            df = df.sort_values(by=['Akamai_Account', 'Account_Type'], key=lambda x: x.str.lower())
            df = df.reset_index(drop=True)
            df.index = df.index + 1

            df['no.'] = df.groupby('Akamai_Account').cumcount() + 1
            console_df = df[['no.', 'Akamai_Account', 'Account_SwitchKey']].copy()
            # logger.warning(f'\n{console_df}')
            print(tabulate(console_df, headers=['no.', 'Akamai_Account', 'Account_SwitchKey'], showindex=True, tablefmt='github'))
            df['Akamai_Account'] = df['Account(Hyperlink)']
            df['Account_SwitchKey'] = df['sk']

            sheet = {}
            sheet['Account_SwitchKey'] = df[columns]
            filepath = 'account_switchkey_summary.xlsx'
            files.write_xlsx(filepath, sheet, freeze_column=3) if not df.empty else None
            files.open_excel_application(filepath, True, df[columns])


def check_read_write_v1(access_levels):
    return 'READ-WRITE' in [x['name'] for x in access_levels]


def check_read_write_v3(access_levels):
    return 'READ-WRITE' in access_levels


def get_api_client(args, logger):
    required = ['CPS',
                'Property Manager (PAPI)',
                'Edge Hostnames API (hapi)',
                'CPcode and Reporting group (cprg)']
    iam = IdentityAccessManagement(args.account_switch_key, args.section, logger=logger)
    resp = iam.get_api_client()

    if resp.ok:
        access_token = resp.json()['accessToken']
        resp_v1 = iam.access_apis_v1(access_token)

        client_name = resp.json()['clientName']
        resp_v3 = iam.access_apis_v3(client_name)

        if not (resp_v1.ok and resp_v3.ok):
            logger.error(print_json(data=resp_v1.json()))
            logger.error(print_json(data=resp_v3.json()))
            return False
        else:
            data_v1 = resp_v1.json()['authorization']['services']
            df_v1 = pd.DataFrame(data_v1)
            df_v1 = df_v1[df_v1['serviceName'].isin(required)].copy()
            df_v1['RW'] = df_v1['grantScopes'].apply(check_read_write_v1)
            all_true = df_v1['RW'].all()
            cols = ['serviceName', 'grantScopes', 'RW']
            logger.debug(f'{df_v1[cols]}')

            data_v3 = resp_v3.json()
            df_v3 = pd.DataFrame(data_v3)
            df_v3 = df_v3[df_v3['apiName'].isin(required)].copy()
            df_v3['RW'] = df_v3['accessLevels'].apply(check_read_write_v3)
            cols = ['apiName', 'accessLevels', 'RW']
            logger.debug(f'{df_v3[cols]}')

    if len(df_v1) < len(df_v3):
        apiName = df_v3['apiName'].values.tolist()
        serviceName = df_v1['serviceName'].values.tolist()
        diff = list(set(apiName) - set(serviceName))
        print()
        logger.error(f'Missing READ-WRITE to API named: {diff}')
        return False
    else:
        logger.info('Valid API access')
        return True
