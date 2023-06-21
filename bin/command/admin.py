from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from akamai_api.identity_access import IdentityAccessManagement
from tabulate import tabulate
from utils._logging import setup_logger
logger = setup_logger()


def homepage_url(account_id: str) -> str:
    url = 'https://control.akamai.com/apps/home-page/#/manage-account?accountId='
    if account_id:
        url = f'{url}{account_id}&targetUrl='
        # this clickable link using escape sequences may not work in all terminal emulators or environments
        return f'\033]8;;{url}\033\\{url}\033]8;;\033\\'
    else:
        return ''


def cleanup_arguments(value: str):
    if value == '*':
        logger.error(f'not allow:             {value:<20}')
        return value
    elif len(value) < 3:
        logger.error(f'minimum 3 characters:  {value:<20}')
        return value


def lookup_account(args):
    iam = IdentityAccessManagement(args.account_switch_key, args.section)
    searches = sorted(args.account)

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
        df['contractTypeId'] = df['accountSwitchKey'].str.split(':').apply(lambda col: col[0] if len(col) > 0 else col)
        df['url'] = df.apply(lambda row: homepage_url(row['accountId']), axis=1)

        # drop header and unwanted columns
        accountSwitchKey = ' ' * 20
        accountName = ' ' * 60
        url = ' ' * 100
        df = df.drop(['accountId', 'contractTypeId'], axis=1)
        columns = [index_header, accountSwitchKey, accountName, url]
        print(tabulate(df, headers=columns, showindex=False, tablefmt='github'))
