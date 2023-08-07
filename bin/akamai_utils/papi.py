from __future__ import annotations

import concurrent.futures
import copy
import json
import logging
import re
import time
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
from akamai_api.papi import Papi
from pandarallel import pandarallel
from rich import print_json
from rich.console import Console
from rich.syntax import Syntax
from utils import dataframe
from utils import files


class PapiWrapper(Papi):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_account_id(self):
        return super().get_account_id()

    def get_contracts(self):
        contracts = super().get_contracts()
        df = pd.DataFrame(contracts)
        df.sort_values(by=['contractId'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        sorted_contracts = sorted([contract['contractId'] for contract in contracts])
        self.logger.info(f'{sorted_contracts=}')
        return sorted_contracts

    def get_edgehostnames(self, contract_id: str, group_id: int):
        return super().get_edgehostnames(contract_id, group_id)

    # GROUPS
    def group_url(self, group_id: int):
        return f'https://control.akamai.com/apps/property-manager/#/groups/{group_id}/properties'

    def create_groups_dataframe(self, groups: list) -> pd.dataframe:
        df = pd.DataFrame(groups)
        df['path'] = df.apply(lambda row: self.build_path(row, groups), axis=1)
        df['level'] = df['path'].str.count('>')
        max_levels = df['level'].max() + 1
        for level in range(max_levels):
            df[f'L{level}'] = df['path'].apply(lambda x: self.get_level_value(x, level))
        return df

    def build_path(self, row, groups: list) -> str:
        path = row['groupName']
        parent_group_id = row.get('parentGroupId')
        while parent_group_id and parent_group_id in [group['groupId'] for group in groups]:
            parent_group = next(group for group in groups if group['groupId'] == parent_group_id)
            path = f"{parent_group['groupName']} > {path}"
            parent_group_id = parent_group.get('parentGroupId')
        return path

    def update_path(self, df, row, column_name):
        '''
        Function to update the path based on contractId
        '''
        if row.name > 0 and row[column_name] == df.at[row.name - 1, column_name]:
            return df.at[row.name - 1, column_name] + '_' + row['contractId']
        elif row.name < len(df) - 1 and row[column_name] == df.at[row.name + 1, column_name]:
            return row[column_name] + '_' + row['contractId']
        return row[column_name]

    def get_level_value(self, path, level):
        path_parts = path.split(' > ')
        if len(path_parts) > level:
            return path_parts[level]
        return ''

    def get_properties_count(self, row):
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
                count += self.get_properties_count_in_group(group_id, contract_id)
        elif isinstance(contract_ids, str):
            if contract_ids == ' ':
                count = 0
            else:
                count = self.get_properties_count_in_group(group_id, contract_ids)
        return count

    def get_valid_contract(self, row) -> str:

        group_id = int(row['groupId'])
        contract_ids = row['contractIds']
        contracts = []
        for contract_id in contract_ids:
            properties = self.get_properties_count_in_group(group_id, contract_id)
            if properties > 0:
                contracts.append(contract_id)

        if len(contracts) == 1:
            return contracts[0]
        elif len(contracts) > 1:
            return contracts
        else:
            return ''

    def get_top_groups(self) -> tuple:
        status, groups = super().get_groups()
        if status == 200:
            df = pd.DataFrame(groups)
            df = df[df['parentGroupId'].isnull()]  # group with empty parent
            df['groupname'] = df['groupName'].str.lower()
            df.sort_values(by=['parentGroupId', 'groupname'], inplace=True, na_position='first')
            df = df.reset_index(drop=True)
            df['order'] = df.index
            df = df.drop(['groupname'], axis=1)
            groups = df['groupId'].unique()
        else:
            groups = []
            df = pd.DataFrame()
        return groups, df

    def get_all_groups(self):
        return super().get_groups()

    def get_groups(self) -> tuple:
        status, groups = super().get_groups()
        if status == 200:
            df = pd.DataFrame(groups)
            df = df[~df['parentGroupId'].isnull()]  # group with parent
            df['groupname'] = df['groupName'].str.lower()
            df.sort_values(by=['parentGroupId', 'groupId'], inplace=True, na_position='first')
            df.reset_index(inplace=True, drop=True)
            df.drop(['groupname'], axis=1, inplace=True)
            groups = df['groupId'].unique()
            self.logger.debug(groups)
        else:
            groups = []
            df = pd.DataFrame()
        return groups, df

    def get_group_name(self, group_id: int) -> str:
        status, groups = super().get_groups()
        if status == 200:
            df = pd.DataFrame(groups)
            df['groupId'] = df['groupId'].astype(int)
            df = df[df['groupId'] == group_id]
            self.logger.debug(f'Group Detail\n{df}')
            try:
                group_name = df['groupName'].values[0]
            except:
                group_name = ''
            return group_name

    def get_group_contract_id(self, group_id: int) -> list:
        status, groups = super().get_groups()
        contract_id = []
        if status == 200:
            df = pd.DataFrame(groups)
            df.sort_values(by=['groupId'], inplace=True)
            # df['groupId'] = df['groupId'].astype(int)
            df = df[df['groupId'] == str(group_id)]
            self.logger.debug(f'Group Detail\n{df}')
            try:
                contract_id = df['contractIds'].values.tolist()[0]
            except:
                contract_id = []

        return contract_id

    def get_parent_group_id(self, group_id: int) -> int:
        '''
        sample
        df['parentGroupId'] = df[['groupId']].parallel_apply(lambda x: papi.get_parent_group_id(*x), axis=1)
        '''
        status, groups = super().get_groups()
        if status == 200:
            df = pd.DataFrame(groups)
            df['groupId'] = df['groupId'].astype(int)
            df = df[df['groupId'] == group_id]
            try:
                parent_group_id = df['parentGroupId'].values[0]
            except:
                parent_group_id = None
            return parent_group_id

    def get_child_group_id(self, parent_group_id: int) -> list:
        status, groups = super().get_groups()
        if status == 200:
            df = pd.DataFrame(groups)
            df['groupId'] = df['groupId'].astype(int)
            df = df[df['parentGroupId'] == str(parent_group_id)]
            try:
                child_group_id = df['groupId'].values.tolist()
            except:
                child_group_id = None
            return child_group_id

    def get_child_groups(self, parent_group_id: int) -> list:
        status, groups = super().get_groups()

        if status == 200:
            df = pd.DataFrame(groups)
            df = df[df['parentGroupId'] == str(parent_group_id)]
            childs = df['groupName'].values.tolist()
            return childs

    def get_properties_count_in_group(self, group_id: int, contract_id: str) -> int:
        properties = self.get_propertyname_per_group(group_id, contract_id)
        return len(properties)

    def get_propertyname_per_group(self, group_id: int, contract_id: str) -> list:
        self.logger.debug(f'{group_id=} {contract_id=}')
        properties_json = super().get_propertyname_per_group(group_id, contract_id)
        property_df = pd.DataFrame(properties_json)
        properties = []
        if not property_df.empty:
            properties = property_df['propertyName'].values.tolist()
        return properties

    def get_properties_detail_per_group(self, group_id: int, contract_id: str) -> pd.DataFrame:
        self.logger.debug(f'{group_id=} {contract_id=}')
        properties_json = super().get_propertyname_per_group(group_id, contract_id)
        property_df = pd.DataFrame(properties_json)
        if not property_df.empty:
            property_df = property_df.sort_values(by='propertyName')
            self.logger.debug(property_df)
        if 'note' in property_df.columns:
            del property_df['note']
        return property_df

    def get_properties_in_group(self, group_id: int | None = None, contract_id: str | None = None) -> tuple:
        df_list = []
        property_count = {}
        if not group_id:
            parent_groups, df = self.get_top_groups()
            for group_id in parent_groups:
                contracts = df[df['groupId'] == group_id]['contractIds'].item()
                group_name = df[df['groupId'] == group_id]['groupName'].item()
                if len(contracts) > 1:
                    count = 0
                    for i, contract_id in enumerate(contracts, 1):
                        self.logger.debug(f'{group_name} {group_id} {contract_id}')
                        properties = super().get_propertyname_per_group(group_id, contract_id)
                        count += len(properties)

                        if not bool(properties):
                            self.logger.debug(f'{group_name} {group_id} {contracts[0]} {properties} no property')
                        else:
                            self.logger.debug(f'Collecting properties for {group_name:<50} {group_id:<10} {contract_id:<10}')
                            property_df = pd.DataFrame(properties)
                            df_list.append(property_df)
                    property_count[group_id] = count
                elif len(contracts) == 1:
                    self.logger.debug(f'Collecting properties for {group_name:<50} {group_id:<10} {contracts[0]:<10}')
                    properties = self.get_propertyname_per_group(group_id, contracts[0])
                    property_count[group_id] = len(properties)
                    if not bool(properties):
                        self.logger.debug(f'{group_name} {group_id} {contracts[0]} {properties} no property')
                    else:
                        property_df = pd.DataFrame(properties)
                        df_list.append(property_df)
        else:
            self.logger.debug(f'Collecting properties for {group_id=} {contract_id=}')
            properties = self.get_propertyname_per_group(group_id, contract_id)
            property_count[group_id] = len(properties)
            property_df = pd.DataFrame(properties)
            df_list.append(property_df)
        return pd.concat(df_list), property_count

    # PROPERTIES
    def get_property_version_latest(self, property_id: int) -> dict:
        return super().get_property_version_latest(property_id)

    def property_url(self, asset_id: int, group_id: int):
        return f'https://control.akamai.com/apps/property-manager/#/property/{asset_id}?gid={group_id}'

    def get_property_hostnames(self, property_id: int) -> list:
        '''
        sample:
        df['hostname'] = df[['propertyId']].parallel_apply(lambda x: papi.get_property_hostnames(*x), axis=1)
        df['hostname_count'] = df['hostname'].str.len()
        '''
        data = super().get_property_hostnames(property_id)
        df = pd.DataFrame(data)

        if 'cnameFrom' not in df.columns:
            # logger.info(f'propertyId {property_id} without cName')
            return []
        else:
            return df['cnameFrom'].unique().tolist()

    def get_property_version_hostnames(self, property_id: int, version: int) -> dict:
        return super().get_property_version_hostnames(property_id, version)

    def get_property_version_full_detail(self, property_id: int, version: int, dict_key: str | None = None):
        data = super().get_property_version_full_detail(property_id, version)
        return data[dict_key]

    def get_property_version_detail(self, property_id: int, version: int, dict_key: str):
        '''
        df['ruleFormat'] = df.parallel_apply(
            lambda row: papi.get_property_version_detail(
            row['propertyId'],
            int(row['productionVersion'])
            if pd.notnull(row['productionVersion']) else row['latestVersion'],
            'ruleFormat'), axis=1)
        '''
        self.logger.debug(f'{property_id} {version} {dict_key}')
        detail = super().get_property_version_detail(property_id, int(version))
        if dict_key == 'updatedDate':
            try:
                propertyName = detail['propertyName']
            except:
                print_json(data=detail)
            assetId = detail['assetId'][4:]
            gid = detail['groupId'][4:]
            acc_url = f'https://control.akamai.com/apps/property-manager/#/property-version/{assetId}/{version}/edit?gid={gid}'
            self.logger.info(f'{propertyName:<46} {acc_url}')
        try:
            return detail['versions']['items'][0][dict_key]
        except:
            print_json(data=detail)
            return property_id

    def find_name_and_xml(self, json_data, target_data, grandparent=None, parent=None):
        if isinstance(json_data, list):
            for item in json_data:
                self.find_name_and_xml(item, target_data, parent=parent, grandparent=grandparent)
        elif isinstance(json_data, dict):
            for key, value in json_data.items():
                if key == 'name':
                    grandparent = parent
                    parent = value
                elif key == 'xml':
                    target_data.append({
                        'name': grandparent,
                        'xml': value
                    })
                if isinstance(value, (dict, list)):
                    self.find_name_and_xml(value, target_data, grandparent=grandparent, parent=parent)

    def find_name_and_openxml(self, json_data, target_data, grandparent=None, parent=None):
        if isinstance(json_data, list):
            for item in json_data:
                self.find_name_and_openxml(item, target_data, parent=parent, grandparent=grandparent)
        elif isinstance(json_data, dict):
            for key, value in json_data.items():
                if key == 'name':
                    grandparent = parent
                    parent = value
                elif 'Xml' in key and isinstance(value, str):
                    target_data.append({
                        'name': grandparent,
                        'openXml': value,
                        'closeXml': json_data.get('closeXml', value)
                    })
                    return  # Continue to the next iteration

                if isinstance(value, (dict, list)):
                    self.find_name_and_openxml(value, target_data, grandparent=grandparent, parent=parent)

    def same_rule(self, properties: dict, first: str, second: str) -> list:
        left = list(properties[first][0].keys())
        right = list(properties[second][0].keys())
        same_rule = list(set(left) & set(right))
        return same_rule

    def different_rule(self, properties: dict, first: str, second: str) -> list:
        left = list(properties[first][0].keys())
        right = list(properties[second][0].keys())
        different_rule = list(set(left) - set(right))
        different_rule.extend(list(set(right) - set(left)))
        return different_rule

    def compare_xml(self, properties: dict, first: str, second: str, rule: str) -> bool:
        try:
            xml_1 = properties[first][0][rule]
        except KeyError:
            xml_1 = 0
            self.logger.info(f' {rule:<30} not found in {first}')
        try:
            xml_2 = properties[second][0][rule]
        except KeyError:
            xml_2 = 0
            self.logger.info(f' {rule:<30} not found in {second}')
        return xml_1 == xml_2

    # WHOLE ACCOUNT
    def account_group_summary(self) -> tuple:
        status_code, all_groups = self.get_all_groups()
        if status_code == 200:
            df = self.create_groups_dataframe(all_groups)
            self.logger.debug(df)
        else:
            return None, None

        df['name'] = df['L0'].str.lower()  # this column will be used for sorting later
        df['groupId'] = df['groupId'].astype(int)  # API has groupId has interger
        columns = ['name', 'groupId']
        if 'parentGroupId' in df.columns.values.tolist():
            df['parentGroupId'] = pd.to_numeric(df['parentGroupId'], errors='coerce').fillna(0)  # API has groupId has interger
            df['parentGroupId'] = df['parentGroupId'].astype(int)
            columns = ['name', 'parentGroupId', 'L1', 'groupId']

        df = df.sort_values(by=columns)
        df = df.drop(['level'], axis=1)
        df = df.fillna('')
        df = df.reset_index(drop=True)

        pandarallel.initialize(progress_bar=False, verbose=0)
        df['account'] = self.account_switch_key
        df['propertyCount'] = df.parallel_apply(lambda row: self.get_properties_count(row), axis=1)
        df['contractId'] = df.parallel_apply(lambda row: self.get_valid_contract(row), axis=1)

        columns = df.columns.tolist()
        levels = [col for col in columns if col.startswith('L')]  # get hierachy

        if 'parentGroupId' in df.columns.values.tolist():
            columns = ['path'] + levels + ['account', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount', 'name']
        else:
            columns = ['path'] + levels + ['account', 'groupName', 'groupId', 'contractId', 'propertyCount', 'name']
        stag_df = df[columns].copy()

        # Split rows some groups/folders have multiple contracts
        stag_df = stag_df.apply(lambda row: dataframe.split_rows(row, column_name='contractId'), axis=1)
        stag_df = pd.concat(list(stag_df), ignore_index=True)

        df = stag_df[columns].copy()
        df = df.reset_index(drop=True)
        df['propertyCount'] = df.parallel_apply(lambda row: self.get_properties_count(row), axis=1)

        allgroups_df = df.copy()

        allgroups_df = allgroups_df.reset_index(drop=True)

        if 'parentGroupId' in allgroups_df.columns.values.tolist():
            columns = ['index_1', 'updated_path', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount'] + levels
        else:
            columns = ['index_1', 'updated_path', 'groupName', 'groupId', 'contractId', 'propertyCount'] + levels

        allgroups_df['updated_path'] = allgroups_df.parallel_apply(lambda row: self.update_path(allgroups_df, row, column_name='path'), axis=1)
        allgroups_df['index_1'] = allgroups_df.index
        allgroups_df = allgroups_df[columns].copy()
        first_non_empty = allgroups_df.replace('', np.nan).ffill(axis=1).iloc[:, -1]
        allgroups_df['excel_sheet'] = ''
        allgroups_df['excel_sheet'] = np.where(first_non_empty == '', allgroups_df['L0'], first_non_empty)

        pattern = r'[A-Z0-9]-[A-Z0-9]+'
        allgroups_df['excel_sheet'] = allgroups_df['excel_sheet'].parallel_apply(lambda x: re.sub(pattern, '', x))
        allgroups_df['excel_sheet'] = allgroups_df['excel_sheet'].parallel_apply(lambda x: files.prepare_excel_sheetname(x))
        allgroups_df = allgroups_df.sort_values(by='excel_sheet')
        allgroups_df = allgroups_df.reset_index(drop=True)
        columns = columns + ['excel_sheet']

        allgroups_df = allgroups_df[columns].copy()
        allgroups_df['sheet'] = ''
        allgroups_df = files.update_sheet_column(allgroups_df)

        if 'parentGroupId' in allgroups_df.columns.values.tolist():
            columns = ['index_1', 'updated_path'] + ['groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount', 'sheet']
        else:
            columns = ['index_1', 'updated_path'] + ['groupName', 'groupId', 'contractId', 'propertyCount', 'sheet']

        allgroups_df = allgroups_df[columns].copy()
        allgroups_df = allgroups_df.sort_values(by='index_1')
        allgroups_df = allgroups_df.reset_index(drop=True)

        if 'parentGroupId' in allgroups_df.columns.values.tolist():
            columns = ['group_structure', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount']
            allgroups_df['parentGroupId'] = allgroups_df['parentGroupId'].astype(str)
        else:
            columns = ['group_structure', 'groupName', 'groupId', 'contractId', 'propertyCount']
        allgroups_df = allgroups_df.rename(columns={'updated_path': 'group_structure'})
        allgroups_df = allgroups_df[columns].copy()
        return allgroups_df, columns

    def property_summary_x(self, df: pd.DataFrame) -> list:
        account_properties = []
        for index, row in df.iterrows():
            msg = f"{index:<5} {row['groupId']:<12} {row['group_structure']:<130}"
            if row['propertyCount'] == 0:
                self.logger.info(f'{msg} no property to collect')
            else:
                self.logger.warning(f"{msg} {row['propertyCount']:>5} properties")
                properties = self.get_properties_detail_per_group(row['groupId'], row['contractId'])

                if not properties.empty:
                    properties['propertyId'] = properties['propertyId'].astype('Int64')
                    properties['groupName'] = row['groupName']  # add group name

                    self.logger.debug(' Collecting hostname')
                    properties['hostname'] = properties[['propertyId']].parallel_apply(lambda x: self.get_property_hostnames(*x), axis=1)
                    properties['hostname_count'] = properties['hostname'].str.len()
                    # show one hostname per list and remove list syntax
                    # properties['hostname'] = properties[['hostname']].parallel_apply(lambda x: ',\n'.join(x.iloc[0]) if not x.empty else '', axis=1)
                    self.logger.debug(properties.head(5)['hostname'])

                    self.logger.debug(' Collecting productId')
                    properties['productId'] = properties.parallel_apply(
                        lambda row: self.get_property_version_detail(row['propertyId'], int(row['productionVersion'])
                                                                    if pd.notnull(row['productionVersion']) else row['latestVersion'],
                                                                    'productId'), axis=1)
                    self.logger.debug(' Collecting ruleFormat')
                    properties['ruleFormat'] = properties.parallel_apply(
                        lambda row: self.get_property_version_detail(row['propertyId'], int(row['productionVersion'])
                                                                    if pd.notnull(row['productionVersion']) else row['latestVersion'],
                                                                    'ruleFormat'), axis=1)

                    self.logger.debug(' Collecting property url')
                    properties['propertyURL'] = properties.parallel_apply(lambda row: self.property_url(row['assetId'], row['groupId']), axis=1)
                    properties['url'] = properties.parallel_apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['propertyURL'], row['propertyName']), axis=1)

                    self.logger.debug(' Collecting updatedDate')
                    # pandarallel.initialize(progress_bar=True, nb_workers=1, verbose=0)
                    properties['updatedDate'] = properties.parallel_apply(lambda row: self.get_property_version_detail(row['propertyId'], row['latestVersion'], 'updatedDate'), axis=1)

                    account_properties.append(properties)
        return account_properties

    def property_summary(self, df: pd.DataFrame, concurrency: int | None = 1) -> list:
        account_properties = []

        def process_row(row):
            msg = f"{row.name:<5} {row['groupId']:<12} {row['group_structure']}"
            if row['propertyCount'] == 0:
                self.logger.info(f'{msg} no property to collect')
            else:
                total = f"{row['propertyCount']:<5} properties"
                self.logger.warning(f'{total:<20} {msg}')
                properties = self.get_properties_detail_per_group(row['groupId'], row['contractId'])

                if not properties.empty:
                    properties['propertyId'] = properties['propertyId'].astype('Int64')
                    properties['groupName'] = row['groupName']

                    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                        # 'collecting hostname'
                        properties['hostname'] = list(executor.map(self.get_property_hostnames, properties['propertyId']))
                        properties['hostname_count'] = properties['hostname'].str.len()
                        time.sleep(3)

                        # 'collecting productId'
                        properties['productId'] = list(executor.map(self.get_property_version_detail, properties['propertyId'],
                                                                    properties['productionVersion'].fillna(properties['latestVersion']),
                                                                    ['productId'] * len(properties)))

                        # 'collecting ruleFormat'
                        properties['ruleFormat'] = list(executor.map(self.get_property_version_detail, properties['propertyId'],
                                                                    properties['productionVersion'].fillna(properties['latestVersion']),
                                                                    ['ruleFormat'] * len(properties)))
                        time.sleep(3)

                        # 'collecting property url'
                        properties['propertyURL'] = list(executor.map(self.property_url, properties['assetId'], properties['groupId']))
                        properties['url'] = list(executor.map(files.make_xlsx_hyperlink_to_external_link,
                                                              properties['propertyURL'], properties['propertyName']))

                        # 'collecting updatedDate'
                        properties['updatedDate'] = list(executor.map(self.get_property_version_detail, properties['propertyId'],
                                                                    properties['latestVersion'],
                                                                    ['updatedDate'] * len(properties)))
                        time.sleep(3)

                    account_properties.append(properties)
        df.apply(process_row, axis=1)
        return account_properties

    # RULETREE
    def get_properties_ruletree_digest(self, property_id: int, version: int):
        '''
        sample
        df['ruleFormat'] = df[['propertyId', 'latestVersion']].parallel_apply(lambda x: papi.get_properties_ruletree_digest(*x), axis=1)
        '''
        return super().get_properties_ruletree_digest(property_id, version)

    def get_property_limit(self, property_id: int, version: int):
        limit, full_ruletree = super().property_rate_limiting(property_id, version)
        return limit, full_ruletree

    def get_property_ruletree(self, property_id: int, version: int, remove_tags: list | None = None):
        status, ruletree = super().property_ruletree(property_id, version, remove_tags)
        if status == 200:
            return ruletree
        else:
            self.logger.error(f'{property_id=} {version=}')
            return 'XXX'

    def get_property_behavior(self, data: dict) -> list:
        behavior_names = []
        if 'behaviors' in data:
            for behavior in data['behaviors']:
                if 'name' in behavior:
                    behavior_names.append(behavior['name'])
        if 'children' in data:
            for child in data['children']:
                behavior_names.extend(self.get_property_behavior(child))
        return behavior_names

    def get_property_advanced_match_xml(self, property_id: int, version: int,
                                    displayxml: bool | None = True,
                                    showlineno: bool | None = False) -> dict:

        ruletree_json = self.get_property_ruletree(property_id, version)
        title = f'{self.property_name}_v{version}'
        self.logger.debug(f'{self.property_name} {property_id=}')
        files.write_json(f'output/ruletree/{title}_ruletree.json', ruletree_json)

        with open(f'output/ruletree/{title}_ruletree.json') as f:
            json_object = json.load(f)

        excel_sheet = f'{self.property_name}_v{version}'
        target_data = []
        self.find_name_and_openxml(ruletree_json, target_data)
        xml_data = {}
        for index, item in enumerate(target_data):
            self.logger.debug(item)
            xml_data[item['name']] = f"{item['openXml']}{item['closeXml']}"
            if displayxml:
                self.logger.warning(f"{index:>3}: {item['name']}")
                print()
                xml_str = f"{item['openXml']}{item['closeXml']}"
                syntax = Syntax(xml_str, 'xml', theme='solarized-dark', line_numbers=showlineno)
                console = Console()
                console.print(syntax)
                print()

        return excel_sheet, xml_data

    def get_property_advanced_behavior_xml(self, property_id: int, version: int,
                                           displayxml: bool | None = True,
                                           showlineno: bool | None = False) -> dict:
        ruletree_json = self.get_property_ruletree(property_id, version)
        title = f'{self.property_name}_v{version}'
        self.logger.debug(f'{self.property_name} {property_id=}')
        Path('output/ruletree').mkdir(parents=True, exist_ok=True)
        files.write_json(f'output/ruletree/{title}_ruletree.json', ruletree_json)

        with open(f'output/ruletree/{title}_ruletree.json') as f:
            json_object = json.load(f)

        excel_sheet = f'{self.property_name}_v{version}'
        target_data = []
        self.find_name_and_xml(ruletree_json, target_data)
        xml_data = {}

        # print_json(data=ruletree_json)
        # logger.debug(target_data)
        print()
        for index, item in enumerate(target_data):
            xml_data[item['name']] = item['xml']
            if displayxml:
                self.logger.warning(f"{index:>3}: {item['name']}")
                print()
                syntax = Syntax(item['xml'], 'xml', theme='solarized-dark', line_numbers=showlineno)
                console = Console()
                console.print(syntax)
                print()
        return excel_sheet, xml_data

    def get_property_advanced_override(self, property_id: int, version: int):
        _, full_ruletree = super().property_rate_limiting(property_id, version)
        try:
            advancedOverride = full_ruletree['rules']['advancedOverride']
            return advancedOverride
        except:
            return None

    def get_property_path_n_behavior(self, json: dict):
        navigation = []
        visited_paths = set()

        def traverse_json(json, path=''):
            if isinstance(json, dict):
                if 'behaviors' in json and len(json['behaviors']) > 0:
                    current_path = f'{path} {json["name"]}'.strip()
                    current_path = current_path.replace('default default', 'default')
                    if current_path not in visited_paths:
                        visited_paths.add(current_path)
                        navigation.append({current_path: json['behaviors']})

                for k, v in json.items():
                    if k in ['children', 'behaviors']:
                        traverse_json(v, f'{path} {json["name"]} {k}')

            elif isinstance(json, list):
                for i, item in enumerate(json):
                    index = i + 1
                    traverse_json(item, f'{path} [{index:>3}] > ')

        traverse_json(json)
        return navigation

    def collect_property_behavior(self, property_name: str, json: dict) -> pd.DataFrame:
        behavior = self.get_property_path_n_behavior(json)

        flat = pd.json_normalize(behavior)
        dx = pd.DataFrame()
        dx = pd.DataFrame(flat)
        dx = dx.melt(var_name='path', value_name='json')
        dx = dx.dropna(subset=['json'])
        dx['property'] = property_name

        behavior = pd.DataFrame()
        behavior = dx.explode('json').reset_index(drop=True)
        behavior['type'] = 'behavior'
        behavior['index'] = behavior.groupby(['property', 'path']).cumcount() + 1
        behavior['path'] = behavior.apply(lambda row: f"{row['path']} [{str(row['index']):>3}]", axis=1)
        behavior['behavior'] = behavior.apply(lambda row: f"{row['json']['name']}", axis=1)
        behavior['custom_behaviorId'] = behavior.apply(lambda row: self.extract_custom_behavior_id(row), axis=1)
        behavior['json_or_xml'] = behavior.apply(lambda row: self.extract_behavior_json(row), axis=1)
        behavior = behavior.rename(columns={'behavior': 'name'})

        columns = ['property', 'path', 'type', 'name', 'json_or_xml', 'custom_behaviorId']
        return behavior[columns]

    def get_property_path_n_criteria(self, json: dict):
        navigation = []
        visited_paths = set()

        def traverse_json(json, path=''):
            if isinstance(json, dict):
                if 'criteria' in json and len(json['criteria']) > 0:
                    current_path = f'{path} {json["name"]}'.strip()
                    if current_path not in visited_paths:
                        visited_paths.add(current_path)
                        navigation.append({current_path: json['criteria']})

                for k, v in json.items():
                    if k in ['children', 'behaviors']:
                        traverse_json(v, f'{path} {json["name"]} {k}')

            elif isinstance(json, list):
                for i, item in enumerate(json):
                    index = i + 1
                    traverse_json(item, f'{path} [{index:>3}] > ')

        traverse_json(json)
        return navigation

    def collect_property_criteria(self, property_name: str, json: dict) -> pd.DataFrame:
        criteria = self.get_property_path_n_criteria(json)
        dx = pd.DataFrame()
        if len(criteria) > 0:
            flat = pd.json_normalize(criteria)
            dx = pd.DataFrame(flat)
            dx = dx.melt(var_name='path', value_name='json')
            dx = dx.dropna(subset=['json'])
            dx['property'] = property_name

        criteria = pd.DataFrame()
        if not dx.empty:
            criteria = dx.explode('json').reset_index(drop=True)
            criteria['type'] = 'criteria'
            criteria['index'] = criteria.groupby(['property', 'path']).cumcount() + 1
            try:
                criteria['name'] = criteria.apply(lambda row: f"{row['json']['name']}", axis=1)
            except:
                print_json(data=json)
                self.logger.warning(criteria)
                self.logger.error(flat)

        if not criteria.empty:
            criteria['json_or_xml'] = criteria.apply(lambda row: self.extract_criteria_json(row), axis=1)
            criteria['path'] = criteria.apply(lambda row: f"{row['path']} [{str(row['index']):>3}]", axis=1)
            columns = ['property', 'path', 'type', 'name', 'json_or_xml']
            return criteria[columns]
        return criteria

    def get_product_schema(self, product_id: str, format_version: str | None = 'latest'):
        status, response = super().get_ruleformat_schema(product_id, format_version)
        if status == 200:
            return response
        else:
            return 'XXX'

    # BEHAVIORS
    def get_behavior(self, rule_dict: dict, behavior: str) -> dict:
        rule_dict = rule_dict['definitions']['catalog']['behaviors']
        matching = [key for key in rule_dict if behavior.lower() in key.lower()]
        if not matching:
            self.logger.critical(f'{behavior} not in catalog')
            return {}
        data = {key: rule_dict[key] for key in matching}
        return data

    def get_behavior_option(self, behavior_dict: dict, behavior: str) -> dict:
        matching = [key for key in behavior_dict if behavior.lower() in key.lower()]
        if not matching:
            self.logger.critical(f'{behavior} not in catalog')
            return {}
        matched_key = matching[0]
        if 'options' not in behavior_dict[matched_key]['properties']:
            self.logger.warning(f'{behavior} has no options')
            return {}
        else:
            value = behavior_dict[matched_key]['properties']['options']['properties']
            if not value:
                self.logger.error(f'{behavior} has no options')
                print()
                return value
            else:
                return value

    def check_behavior(self, behaviors: list, df: pd.DataFrame, cpcode):
        for behavior in behaviors:
            self.logger.debug(behavior)
            if behavior == 'origin':
                df[behavior] = df.parallel_apply(lambda row: self.origin_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df[f'{behavior}_count'] = df[behavior].str.len()
                df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            elif behavior == 'siteshield':
                df[behavior] = df.parallel_apply(lambda row: self.siteshield_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            elif behavior == 'sureroute':
                df[behavior] = df.parallel_apply(lambda row: self.sureroute_value_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            elif behavior == 'custombehavior':
                try:
                    df[behavior] = df.parallel_apply(lambda row: self.custom_behavior_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                    df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
                except:
                    self.logger.error(behavior)
            elif behavior == 'cpcode':
                df[behavior] = df.parallel_apply(lambda row: self.cpcode_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df['cpcode_count'] = df[behavior].str.len()
                df[f'{behavior}_name'] = df['cpcode'].parallel_apply(lambda x: [cpcode.get_cpcode_name(cp) for cp in x])
                df[behavior] = df[[behavior]].parallel_apply(
                    lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
                df['cpcode_name'] = df[['cpcode_name']].parallel_apply(
                    lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            else:
                df[behavior] = df.parallel_apply(
                    lambda row: self.behavior_count(row['propertyName'],
                                                    row['ruletree']['rules'], behavior), axis=1)

        return df

    @staticmethod
    def behavior_count(property_name: str, rules: dict, target_behavior: str):
        parent_count = 0

        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'].lower() == target_behavior.lower():
                    parent_count += 1
            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_count = PapiWrapper.behavior_count(property_name, child_rule, target_behavior)
                    parent_count += child_count
        return parent_count

    def cpcode_value(self, property_name: str, rules: dict):
        values = []

        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'] == 'cpCode':
                    try:
                        values.append(behavior['options']['value']['id'])
                    except:
                        self.logger.error(f'{property_name} cpCode not found')
                elif behavior['name'] == 'failAction':  # Site Failover
                    try:
                        values.append(behavior['options']['cpCode']['id'])
                    except:
                        # logger.warning(f'{property_name:<40} cpCode not found for Site Failover')
                        pass

                elif behavior['name'] == 'visitorPrioritization':
                    try:
                        values.append(behavior['options']['waitingRoomCpCode']['cpCode'])
                    except:
                        pass
                    try:
                        values.append(behavior['options']['waitingRoomNetStorage']['cpCode'])
                    except:
                        pass

            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.cpcode_value(property_name, child_rule)
                    values.extend(child_values)
        return list(set(values))

    def custom_behavior_value(self, property_name: str, rules: dict):
        values = []
        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'] == 'customBehavior':
                    try:
                        values.append(behavior['options']['behaviorId'])
                    except:
                        self.logger.error(f'{property_name:<40} behaviorId not found')
            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.custom_behavior_value(property_name, child_rule)
                    values.extend(child_values)
        return list(set(values))

    def origin_value(self, property_name: str, rules: dict):
        values = []
        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'] == 'origin':
                    try:
                        values.append(behavior['options']['hostname'])
                    except:
                        pass
                    try:
                        values.append(behavior['options']['netStorage']['downloadDomainName'])
                    except:
                        pass

            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.origin_value(property_name, child_rule)
                    values.extend(child_values)
        return sorted(list(set(values)))

    def siteshield_value(self, property_name: str, rules: dict):
        values = []
        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'] == 'siteShield':
                    try:
                        values.append(behavior['options']['ssmap']['value'])
                    except:
                        self.logger.error(f'{property_name:<40} siteShield not found')
            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.siteshield_value(property_name, child_rule)
                    values.extend(child_values)
        return sorted(list(set(values)))

    def sureroute_value(self, property_name: str, rules: dict):
        values = []
        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'] == 'siteShield':
                    try:
                        values.append(behavior['options']['ssmap']['srmap'])
                    except:
                        self.logger.error(f'{property_name} sureRoute map not found')
            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.sureroute_value(property_name, child_rule)
                    values.extend(child_values)
        return sorted(list(set(values)))

    # ACTIVATION
    def activate_property_version(self, property_id: int, version: int, network: str, note: str, emails: list):
        status, response = super().activate_property_version(property_id, version, network, note, emails)
        if status == 201:
            try:
                activation_id = int(response.split('?')[0].split('/')[-1])
                return int(activation_id)
            except:
                self.logger.warning(activation_id)
                return 0
        return status

    def activation_status(self, property_id: int, activation_id: int, version: int):

        if activation_id > 0 and version > 0:
            status, response = super().activation_status(property_id, activation_id)
            self.logger.debug(f'{activation_id=} {version=} {property_id=} {status}')
            df = pd.DataFrame(response)
            self.logger.debug(f'BEFORE\n{df}')
            df = df[df['propertyVersion'] == version].copy()
            self.logger.debug(f'FILTERED\n{df}')
            return df.status.values[0]
        else:
            return ' '

    # CUSTOM BEHAVIOR
    def list_custom_behaviors(self):
        return super().list_custom_behaviors()

    def get_custom_behaviors(self, id: str):
        return super().get_custom_behaviors(id)

    # HELPER
    def extract_criteria_json(self, row) -> str:
        if row['name'] == 'matchAdvanced':
            openXml = row['json']['options']['openXml']
            closeXml = row['json']['options']['closeXml']
            return f'{openXml}{closeXml}'
        else:
            return row['json']['options']

    def extract_behavior_json(self, row) -> str:
        if row['behavior'] == 'customBehavior':
            return self.get_custom_behaviors(row['custom_behaviorId'])[1]
        if row['behavior'] == 'advanced':
            return row['json']['options']['xml']
        else:
            return row['json']['options']

    def extract_custom_behavior_id(self, row) -> str:
        if row['behavior'] == 'customBehavior':
            return row['json']['options']['behaviorId']
        else:
            return ''


class Node:
    def __init__(self, name, value, parent=None):
        self.name = name
        self.value = value
        self.parent = parent

    def get_path(self):
        if self.parent is None:
            return self.name
        else:
            return f'{self.parent.get_path()} > {self.name}'


if __name__ == '__main__':
    pass
