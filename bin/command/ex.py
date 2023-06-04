from __future__ import annotations

import re

import numpy as np
import pandas as pd
from akamai_utils import papi as p
from pandarallel import pandarallel
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import files
logger = lg.setup_logger()


def account_group_summary(args):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    _, all_groups = papi.get_all_groups()

    df = create_dataframe(all_groups)

    df['name'] = df['L0'].str.lower()

    df['groupId'] = df['groupId'].astype(int)
    df['parentGroupId'] = pd.to_numeric(df['parentGroupId'], errors='coerce').fillna(0)
    df['parentGroupId'] = df['parentGroupId'].astype(int)

    df = df.sort_values(by=['name', 'parentGroupId', 'L1', 'groupId'])
    df = df.drop(['groupName', 'level'], axis=1)
    df = df.fillna('')
    df = df.reset_index(drop=True)

    pandarallel.initialize(progress_bar=False)
    df['account'] = args.account_switch_key
    df['propertyCount'] = df.parallel_apply(lambda row: get_properties_count(row), axis=1)
    df['contractId'] = df.parallel_apply(lambda row: get_valid_contract(row, papi), axis=1)

    columns = df.columns.tolist()
    levels = [col for col in columns if col.startswith('L')]
    columns = ['path'] + levels + ['account', 'groupId', 'parentGroupId', 'contractId', 'propertyCount', 'name']
    # columns = ['path'] + ['account', 'groupId', 'parentGroupId', 'contractId',  'propertyCount']
    stag_df = df[columns].copy()
    # print(tabulate(stag_df, headers=columns, showindex=True, tablefmt='simple'))

    # Split rows with list contractId values
    stag_df = stag_df.apply(lambda row: split_rows(row), axis=1)
    stag_df = pd.concat(list(stag_df), ignore_index=True)

    df = stag_df[columns].copy()
    df = df.reset_index(drop=True)
    df['propertyCount'] = df.parallel_apply(lambda row: get_properties_count(row), axis=1)

    group_df = df[df['propertyCount'] > 0].copy()
    group_df = group_df.reset_index(drop=True)
    # print(tabulate(group_df, headers=columns, showindex=True, tablefmt='simple'))

    column_name = 'path'
    group_df['updated_path'] = group_df.apply(lambda row: update_path(group_df, row, column_name), axis=1)
    columns = ['index_1', 'updated_path'] + ['groupId', 'parentGroupId', 'contractId', 'propertyCount'] + levels
    group_df['index_1'] = group_df.index
    group_df = group_df[columns].copy()
    # print(tabulate(group_df, headers=columns, showindex=True, tablefmt='simple'))

    group_df['excel_sheet'] = ''
    first_non_empty = group_df.replace('', np.nan).ffill(axis=1).iloc[:, -1]
    group_df['excel_sheet'] = np.where(first_non_empty == '', group_df['L0'], first_non_empty)
    print(tabulate(group_df, headers=columns, showindex=True, tablefmt='simple'))

    pattern = r'[A-Z0-9]-[A-Z0-9]+'
    # Remove the specified strings using regular expressions
    group_df['excel_sheet'] = group_df['excel_sheet'].apply(lambda x: re.sub(pattern, '', x))
    group_df['excel_sheet'] = group_df['excel_sheet'].apply(lambda x: files.prepare_excel_sheetname(x))
    group_df = group_df.sort_values(by='excel_sheet')
    group_df = group_df.reset_index(drop=True)
    columns = columns + ['excel_sheet']

    excel_df = group_df[columns].copy()
    excel_df['sheet'] = ''
    excel_df = update_sheet_column(excel_df)
    columns = ['index_1', 'updated_path'] + ['groupId', 'parentGroupId', 'contractId', 'propertyCount', 'sheet']

    excel_df = excel_df[columns].copy()
    excel_df = excel_df.sort_values(by='index_1')
    excel_df = excel_df.reset_index(drop=True)

    columns = ['updated_path'] + ['groupId', 'parentGroupId', 'contractId', 'propertyCount', 'sheet']
    excel_df = excel_df[columns].copy()
    print()
    print(tabulate(excel_df, headers=columns, showindex=True, tablefmt='github'))
    print()
    logger.warning(df.shape)
    logger.error(excel_df.shape)


def update_path(df, row, column_name):
    '''
    Function to update the path based on contractId
    '''
    if row.name > 0 and row[column_name] == df.at[row.name - 1, column_name]:
        return df.at[row.name - 1, column_name] + '_' + row['contractId']
    elif row.name < len(df) - 1 and row[column_name] == df.at[row.name + 1, column_name]:
        return row[column_name] + '_' + row['contractId']
    return row[column_name]


def update_sheet_column(df):
    '''
    Function to update the sheet column based on contractId
    '''
    df['sheet'] = df['excel_sheet']
    duplicate_mask = df['excel_sheet'].duplicated(keep=False)
    df.loc[duplicate_mask, 'sheet'] += '_' + df.loc[duplicate_mask, 'contractId']
    return df


def create_dataframe(groups):
    df = pd.DataFrame(groups)
    df['path'] = df.apply(lambda row: build_path(row, groups), axis=1)
    df['level'] = df['path'].str.count('>')
    max_levels = df['level'].max() + 1
    for level in range(max_levels):
        df[f'L{level}'] = df['path'].apply(lambda x: get_level_value(x, level))
    return df


def build_path(row, groups):
    path = row['groupName']
    parent_group_id = row.get('parentGroupId')
    while parent_group_id and parent_group_id in [group['groupId'] for group in groups]:
        parent_group = next(group for group in groups if group['groupId'] == parent_group_id)
        path = f"{parent_group['groupName']} > {path}"
        parent_group_id = parent_group.get('parentGroupId')
    return path


def get_level_value(path, level):
    path_parts = path.split(' > ')
    if len(path_parts) > level:
        return path_parts[level]
    return ''


def get_properties_count(row):
    papi = p.PapiWrapper(account_switch_key=row['account'])
    group_id = int(row['groupId'])
    if 'contractIds' in list(row.index.values):
        contract_ids = row['contractIds']
    elif 'contractId' in list(row.index.values):
        try:
            contract_ids = row['contractId']
        except:
            contract_ids = None
    count = 0
    if contract_ids == 0 or contract_ids == '' or contract_ids is None:
        count = 0
    elif isinstance(contract_ids, list):
        for contract_id in contract_ids:
            count += papi.get_properties_count_in_group(group_id, contract_id)
    elif isinstance(contract_ids, str):
        if contract_ids == ' ':
            count = 0
        else:
            count = papi.get_properties_count_in_group(group_id, contract_ids)
    return count


def get_valid_contract(row, papi) -> str:

    group_id = int(row['groupId'])
    contract_ids = row['contractIds']
    contracts = []
    for contract_id in contract_ids:
        properties = papi.get_properties_count_in_group(group_id, contract_id)
        if properties > 0:
            contracts.append(contract_id)

    if len(contracts) == 1:
        return contracts[0]
    elif len(contracts) > 1:
        return contracts
    else:
        return ''


# Function to split rows when contractId is a list
def split_rows(row):
    contract_id = row['contractId']
    if isinstance(contract_id, list):
        rows = []
        for id in contract_id:
            new_row = row.copy()
            new_row['contractId'] = id
            rows.append(new_row)
        return pd.DataFrame(rows)
    else:
        return pd.DataFrame([row])


def df_folder(groups, parent_group_id=None, parent_name='', level=0):
    result = []
    for group in groups:
        group_id = group['groupId']
        group_name = group['groupName']
        parent_id = group.get('parentGroupId')

        if parent_id == parent_group_id:
            name = f'{parent_name} > {group_name}' if parent_name else group_name
            result.append({
                'level': level,
                'path': name,
                'groupId': group_id,
                'parentId': parent_group_id
            })
            result.extend(df_folder(groups, parent_group_id=group_id, parent_name=name, level=level + 1))

    return result


def build_folder_structure(data, parent_group_id=None, level=0):

    # from akamai_utils import papi as p
    # papi = p.PapiWrapper(account_switch_key=args.account)
    # _, all_groups = papi.get_all_groups()
    '''
        folder_structure = build_folder_structure(all_groups)
    for folder_path, level in folder_structure:
        indent = '    ' * level
        print(f"{level}: {indent}{folder_path}")
    '''
    for item in data:
        group_name = item['groupName']
        group_id = item['groupId']
        if item.get('parentGroupId') == parent_group_id:
            if parent_group_id is None:
                yield f'{group_name:<50} {group_id}', level
            else:
                indent = '    ' * level
                yield f'{indent}> {group_name:<50} {group_id}', level
            yield from build_folder_structure(data, group_id, level + 1)
