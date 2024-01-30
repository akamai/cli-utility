from __future__ import annotations

import concurrent.futures
import copy
import json
import logging
import re
import sys
import time
from pathlib import Path
from time import perf_counter
from typing import Any
from typing import Dict
from typing import Optional
from typing import SupportsIndex

import emojis
import numpy as np
import pandas as pd
from akamai_api.papi import Papi
from akamai_utils.cpcode import CpCodeWrapper
from jsonpath_ng.ext import parse
from pandarallel import pandarallel
from pandas import DataFrame
from rich import print_json
from rich.console import Console
from rich.syntax import Syntax
from utils import _logging as lg
from utils import dataframe
from utils import files


class PapiWrapper(Papi):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):  # type: ignore
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_account_id(self) -> str:
        return super().get_account_id()

    def get_contracts(self) -> list[str]:
        contracts = super().get_contracts()
        df = pd.DataFrame(contracts)
        df.sort_values(by=['contractId'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        sorted_contracts = sorted([contract['contractId'] for contract in contracts])
        self.logger.info(f'{sorted_contracts=}')
        return sorted_contracts

    def get_edgehostnames(self, contract_id: str, group_id: int) -> list[str]:
        return super().get_edgehostnames(contract_id, group_id)

    def get_account_hostnames(self) -> list[str]:
        return super().get_account_hostnames()

    def list_bulk_search(self, id: int):
        return super().list_bulk_search(id)

    def list_bulk_patch(self, id: int):
        return super().list_bulk_patch(id)

    def list_bulk_activation(self, id: int):
        return super().list_bulk_activation(id)

    def list_bulk_create(self, id: int):
        return super().list_bulk_create(id)

    def bulk_search(self, query: dict):
        resp = super().bulk_search_properties(query)
        if not resp.ok:
            self.logger.error(print_json(data=resp.json()))
        else:
            return_url = resp.json()['bulkSearchLink']
            pattern = r'\/papi\/v1\/bulk\/rules-search-requests\/(\d+)'
            match = re.search(pattern, return_url)
            if match is None:
                self.logger.error('bulk_id not found in the URL')
            else:
                bulk_id = match.group(1)
                count = 0
                status = 'initial'
                while (status != 'COMPLETE'):
                    count += 1
                    _resp = super().list_bulk_search(bulk_id)
                    # print_json(data=_resp.json())
                    status = _resp.json()['searchTargetStatus']
                    if count > 5:
                        continue

            self.logger.critical(f'bulkSearchId: {bulk_id}')

            return _resp

    def bulk_create_properties(self, property: list[str, int]):
        resp = super().bulk_create_properties(property)
        if resp.ok:
            return_url = resp.json()['bulkCreateVersionLink']
            pattern = r'\/papi\/v1\/bulk\/property-version-creations\/(\d+)'
            match = re.search(pattern, return_url)
            if match:
                bulk_id = match.group(1)
                self.logger.critical(f'bulkCreateId: {bulk_id}')
                count = 0
                status = 'initial'
                while (status != 'COMPLETE'):
                    count += 1
                    _resp = super().list_bulk_create(bulk_id)
                    status = _resp.json()['bulkCreateVersionsStatus']
                    if count > 20:
                        break
                resp = _resp
            else:
                self.logger.error('bulk_id not found in the URL')
        else:
            self.logger.error(print_json(data=resp.json()))
        return resp

    def bulk_update_behavior(self, property: list, patch_json: dict):
        resp = super().bulk_update_behavior(property, patch_json)
        self.logger.debug(resp.status_code)
        if not resp.ok:
            self.logger.error(print_json(data=resp.json()))
        else:
            return_url = resp.json()['bulkPatchLink']
            bulk_id = int(return_url.split('?')[0].split('/')[-1])
            if bulk_id:
                count = 0
                status = 'initial'
                while (status != 'COMPLETE'):
                    count += 1
                    _resp = super().list_bulk_patch(bulk_id)
                    status = _resp.json()['bulkPatchStatus']
                    if count > 5:
                        break
                resp = _resp
            else:
                self.logger.error('bulk_id not found in the URL')

        self.logger.critical(f'bulkPatchId: {bulk_id}')
        return resp

    def bulk_activate_properties(self, network: str, email: list, pr_email: str, note: str, properties: list):
        resp = super().bulk_activate_properties(network, email, pr_email, note, properties)
        self.logger.debug(f'{resp.status_code} {resp.url}')
        if not resp.ok:
            self.logger.error(print_json(data=resp.json()))
        else:
            return_url = resp.json()['bulkActivationLink']
            activation_id = int(return_url.split('?')[0].split('/')[-1])
            if activation_id:
                status = 'initial'
                count = 0
                while (status != 'COMPLETE') and count <= 5:
                    count += 1
                    _resp = super().list_bulk_activation(activation_id)
                    status = _resp.json()['bulkActivationStatus']
                    if count > 5:
                        continue
                resp = _resp
            else:
                self.logger.error('bulk_id not found in the URL')
        self.logger.critical(f'bulkActivationId: {activation_id}')
        return resp

    def bulk_add_rule(self, property: list[str, int], patch_json: dict):
        resp = super().bulk_add_rule(property, patch_json)
        if not resp.ok:
            self.logger.error(print_json(data=resp.json()))
        else:
            return_url = resp.json()['bulkPatchLink']
            bulk_id = int(return_url.split('?')[0].split('/')[-1])
            if bulk_id:
                count = 0
                status = 'initial'
                self.logger.critical(f'bulkPatchId: {bulk_id}')
                while (status != 'COMPLETE'):
                    count += 1
                    _resp = super().list_bulk_patch(bulk_id)
                    status = _resp.json()['bulkPatchStatus']
                    if count > 5:
                        continue
                resp = _resp
            else:
                self.logger.error('bulk_id not found in the URL')

        return resp

    # GROUPS
    def group_url(self, group_id: int) -> str:
        return f'https://control.akamai.com/apps/property-manager/#/groups/{group_id}/properties'

    def create_groups_dataframe(self, groups: list[str]) -> DataFrame:
        df = pd.DataFrame(groups)
        df['path'] = df.apply(lambda row: self.build_path(row, groups), axis=1)
        df['level'] = df['path'].str.count('>')
        max_levels = df['level'].max() + 1
        for level in range(max_levels):
            df[f'L{level}'] = df['path'].apply(lambda x: self.get_level_value(x, level))
        return df

    def build_path(self, row: pd.Series[str], groups: list[str]) -> str:
        path = row['groupName']
        parent_group_id = row['parentGroupId'] if 'parentGroupId' in row else 0
        while parent_group_id and parent_group_id in [group['groupId'] for group in groups]:
            parent_group = next(group for group in groups if group['groupId'] == parent_group_id)
            path = f"{parent_group['groupName']} > {path}"
            parent_group_id = parent_group.get('parentGroupId')
        return path

    def update_path(self, df: pd.DataFrame, row: pd.Series, column_name: str) -> str:
        '''
        Function to update the path based on contractId
        '''
        if row.name > 0 and row[column_name] == df.at[row.name - 1, column_name]:
            return df.at[row.name - 1, column_name] + '_' + row['contractId']
        elif row.name < len(df) - 1 and row[column_name] == df.at[row.name + 1, column_name]:
            return row[column_name] + '_' + row['contractId']
        return row[column_name]

    def get_level_value(self, path: str, level: int) -> str:
        path_parts = path.split(' > ')
        if len(path_parts) > level:
            return path_parts[level]
        return ''

    def get_properties_count(self, row: pd.Series) -> int:
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

    def get_valid_contract(self, row: pd.Series) -> list[str]:

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
            return []

    def get_top_groups(self) -> tuple[list[int], DataFrame]:
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

    def get_all_groups(self) -> tuple[int, str]:
        return super().get_groups()

    def get_groups(self) -> tuple[list[int], DataFrame]:
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
            df = df[df['groupId'] == group_id]
            self.logger.debug(f'Group Detail\n{df}')
            try:
                group_name = df['groupName'].values[0]
            except:
                group_name = ''
            return group_name

    def get_group_contract_id(self, group_id: int) -> list[str]:
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
        parent_group_id = 0
        if status == 200:
            df = pd.DataFrame(groups)
            df['groupId'] = df['groupId'].astype(int)
            df = df[df['groupId'] == group_id]
            try:
                parent_group_id = df['parentGroupId'].values[0]
            except:
                pass
        return parent_group_id

    def get_child_group_id(self, parent_group_id: int) -> list[int]:
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

    def get_child_groups(self, parent_group_id: int) -> list[str]:
        status, groups = super().get_groups()

        if status == 200:
            df = pd.DataFrame(groups)
            df = df[df['parentGroupId'] == str(parent_group_id)]
            childs = df['groupName'].values.tolist()
            return childs

    def get_properties_count_in_group(self, group_id: int, contract_id: str) -> int:
        properties = self.get_propertyname_per_group(group_id, contract_id)
        return len(properties)

    def get_propertyname_per_group(self, group_id: int, contract_id: str) -> list[str]:
        self.logger.debug(f'{group_id=} {contract_id=}')
        properties_json = super().get_propertyname_per_group(group_id, contract_id)
        property_df = pd.DataFrame(properties_json)
        properties = []
        if not property_df.empty:
            properties = property_df['propertyName'].values.tolist()
        return properties

    def get_properties_detail_per_group(self, group_id: int, contract_id: str) -> DataFrame:
        self.logger.debug(f'{group_id=} {contract_id=}')
        properties_json = super().get_propertyname_per_group(group_id, contract_id)
        property_df = pd.DataFrame(properties_json)
        if not property_df.empty:
            property_df = property_df.sort_values(by='propertyName')
            self.logger.debug(property_df)
        if 'note' in property_df.columns:
            del property_df['note']
        return property_df

    def get_properties_in_group(self, group_id: int | None = 0, contract_id: str | None = '') -> tuple:
        df_list = []
        property_count = {}
        if group_id == 0:
            parent_groups, df = self.get_top_groups()
            for _group_id in parent_groups:
                contracts = df[df['groupId'] == _group_id]['contractIds'].item()
                group_name = df[df['groupId'] == _group_id]['groupName'].item()
                if len(contracts) > 1:
                    count = 0
                    for i, contract_id in enumerate(contracts, 1):
                        self.logger.debug(f'{group_name} {_group_id} {contract_id}')
                        properties = super().get_propertyname_per_group(_group_id, contract_id)
                        count += len(properties)

                        if not bool(properties):
                            self.logger.debug(f'{group_name} {_group_id} {contracts[0]} {properties} no property')
                        else:
                            self.logger.debug(f'Collecting properties for {group_name:<50} {_group_id:<10} {contract_id:<10}')
                            property_df = pd.DataFrame(properties)
                            df_list.append(property_df)
                    property_count[group_id] = count
                elif len(contracts) == 1:
                    self.logger.debug(f'Collecting properties for {group_name:<50} {_group_id:<10} {contracts[0]:<10}')
                    properties = self.get_propertyname_per_group(_group_id, contracts[0])
                    property_count[_group_id] = len(properties)
                    if not bool(properties):
                        self.logger.debug(f'{group_name} {_group_id} {contracts[0]} {properties} no property')
                    else:
                        property_df = pd.DataFrame(properties)
                        df_list.append(property_df)
        else:
            _group_id = group_id
            _contract_id = contract_id
            self.logger.debug(f'Collecting properties for {_group_id=} {_contract_id=}')
            properties = self.get_propertyname_per_group(_group_id, _contract_id)
            property_count[_group_id] = len(properties)
            property_df = pd.DataFrame(properties)
            df_list.append(property_df)
        return pd.concat(df_list), property_count

    # SEARCH
    def search_property_version(self, json: dict) -> tuple[int, int]:
        return super().property_version(json)

    def search_property_by_name(self, property_name: str) -> tuple[int, str]:
        return super().search_property_by_name(property_name)

    def search_property_by_hostname(self, hostname: str) -> str:
        return super().search_property_by_hostname(hostname)

    # PROPERTIES
    def get_property_version_latest(self, property_id: int) -> dict:
        return super().get_property_version_latest(property_id)

    def property_url(self, asset_id: int, group_id: int) -> str:
        return f'https://control.akamai.com/apps/property-manager/#/property/{asset_id}?gid={group_id}'

    def property_url_edit_version(self, asset_id: int, version: int, group_id: int) -> str:
        return f'https://control.akamai.com/apps/property-manager/#/property-version/{asset_id}/{version}/edit?gid={group_id}'

    def build_propertyname_with_version(self, row) -> str:
        property_name = row.propertyName
        production_version = int(row.productionVersion) if not pd.isna(row.productionVersion) else None
        latest_version = int(row.latestVersion) if not pd.isna(row.latestVersion) else None

        if production_version is not None:
            return f'{property_name}_v{production_version}'
        elif latest_version is not None:
            return f'{property_name}_v{latest_version}'
        else:
            return property_name

    def get_property_hostnames(self, property_id: int) -> list[str]:
        '''
        sample:
        df['hostname'] = df[['propertyId']].parallel_apply(lambda x: papi.get_property_hostnames(*x), axis=1)
        df['hostname_count'] = df['hostname'].str.len()
        '''
        hostnames = []
        data = super().get_property_hostnames(property_id)
        if data and len(data) > 0:
            df = pd.DataFrame(data)
            if 'cnameFrom' not in df.columns:
                self.logger.info(f'{property_id=} without cName')
            else:
                hostnames = df['cnameFrom'].unique().tolist()
        else:
            self.logger.debug(f'{property_id=} no hostname')
        return hostnames

    def get_property_version_hostnames(self, property_id: int, version: int) -> dict:
        return super().get_property_version_hostnames(property_id, version)

    def get_property_version_full_detail(self, property_id: int, version: int, dict_key: str | None = None):
        self.logger.debug(f'{property_id} {version}')
        data = super().get_property_version_full_detail(property_id, version)
        return data[dict_key]

    def get_property_version_detail_json(self, property_id: int, version: int):
        return super().get_property_version_full_detail(property_id, version)

    def get_property_version_detail(self, property_id: int, version: int, dict_key: str) -> int:
        '''
        df['ruleFormat'] = df.parallel_apply(
            lambda row: papi.get_property_version_detail(
            row['propertyId'],
            int(row['productionVersion'])
            if pd.notnull(row['productionVersion']) else row['latestVersion'],
            'ruleFormat'), axis=1)
        '''
        detail = super().get_property_version_detail(property_id, int(version))
        if dict_key == 'updatedDate':
            try:
                propertyName = detail['propertyName']
                assetId = detail['assetId']
                gid = detail['groupId']
                acc_url = f'https://control.akamai.com/apps/property-manager/#/property-version/{assetId}/{version}/edit?gid={gid}'
                self.logger.debug(f'{propertyName:<40} {acc_url}')
            except KeyError as e:
                self.logger.error(f'{property_id} {version} {dict_key} missing {str(e)}')
                print_json(data=detail)

        try:
            return detail['versions']['items'][0][dict_key]
        except:
            print_json(data=detail)
            return property_id

    def create_new_property_version(self, property_id: str, base_version: int) -> int:
        return super().create_new_property_version(property_id, base_version)

    def add_shared_ehn(self, property_id: str, version: int):
        return super().add_shared_ehn(property_id, version)

    def find_name_and_xml(self,
                          json_data: dict[str, Any],
                          target_data: list[str],
                          grandparent: str | None = None,
                          parent: str | None = None):
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

    def find_name_and_openxml(self,
                              json_data: dict[str, Any],
                              target_data: list[str],
                              grandparent: str | None = None,
                              parent: str | None = None):
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

    def same_rule(self, properties: dict[str, dict[str, Any]], first: str, second: str) -> list[str]:
        left = list(properties[first][0].keys())
        right = list(properties[second][0].keys())
        same_rule = list(set(left) & set(right))
        return same_rule

    def different_rule(self, properties: dict[str, dict[str, Any]], first: str, second: str) -> list[str]:
        left = list(properties[first][0].keys())
        right = list(properties[second][0].keys())
        different_rule = list(set(left) - set(right))
        different_rule.extend(list(set(right) - set(left)))
        return different_rule

    def compare_xml(self, properties: dict[str, dict[str, Any]], first: str, second: str, rule: str) -> bool:
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
    def account_group_summary(self) -> tuple[DataFrame, list[str]]:
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

    def property_summary(self, df: pd.DataFrame, concurrency: int | None = 1) -> list:
        account_properties = []

        def process_row(row: pd.Series):
            msg = f"{row.name:<5} {row['groupId']:<13} {row['group_structure']}"
            if row['propertyCount'] == 0:
                self.logger.info(f'{msg} no property to collect')
            else:
                total = f"{row['propertyCount']:<5} properties"
                self.logger.warning(f'{total:<20} {msg}')
                properties = self.get_properties_detail_per_group(row['groupId'], row['contractId'])

                if not properties.empty:
                    properties['propertyId'] = properties['propertyId'].astype('Int64')
                    properties['groupName'] = row['group_structure']

                    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                        # 'append _vXXX to propertyName
                        properties['property_with_version'] = list(executor.map(self.build_propertyname_with_version, properties.itertuples()))

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

                        # 'determining environment type'
                        properties['env'] = list(executor.map(self.guestimate_env_type, properties['propertyName']))

                        time.sleep(3)

                    account_properties.append(properties)
        df.apply(process_row, axis=1)
        return account_properties

    def guestimate_env_type(self, name: str):
        lower = ['-qa', '-it', 'stg', 'test', '-stage.', 'stage-', 'staging', '.stage.', '-dev-', 'nonprod', '.dev.', '.qa.']
        return 'nonprd' if any(substring in name for substring in lower) else 'prd'

    # RULETREE
    def get_properties_ruletree_digest(self, property_id: int, version: int) -> dict:
        '''
        sample
        df['ruleFormat'] = df[['propertyId', 'latestVersion']].parallel_apply(lambda x: papi.get_properties_ruletree_digest(*x), axis=1)
        '''
        return super().get_properties_ruletree_digest(property_id, version)

    def get_property_limit(self, property_id: int, version: int) -> tuple[int, dict]:
        limit, full_ruletree = super().property_rate_limiting(property_id, version)
        return limit, full_ruletree

    def get_property_ruletree(self, property_id: int, version: int, remove_tags: list | None = None) -> dict:
        status, ruletree = super().property_ruletree(property_id, version, remove_tags)
        if status == 200:
            return ruletree
        else:
            self.logger.error(f'{property_id=} {version=}')
            return {}

    def get_property_full_ruletree(self, property_id: int, version: int):
        return super().get_property_full_ruletree(property_id, version)

    def update_property_ruletree(self, property_id: int, version: int, rule_format: str, payload: dict, version_notes: str) -> str:
        self.logger.debug(f'{property_id} {version} {rule_format=}')
        status, resp = super().update_property_ruletree(property_id, version, rule_format, payload, version_notes)
        if status != 200:
            self.logger.error(f'{property_id=} {version=} {resp}')
        return status

    def build_new_ruletree(self, ruletree: dict, new_rule: dict) -> str:
        ruletree['rules']['children'].insert(0, new_rule)
        # print_json(data=ruletree)
        chidren = len(ruletree['rules']['children'])
        self.logger.critical(f'after {chidren}')
        return ruletree

    def get_property_behavior(self, data: dict) -> list[str]:
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
                                        showlineno: bool = False) -> tuple[str, dict[str, str]]:
        ruletree_json = self.get_property_ruletree(property_id, version)
        title = f'{self.property_name}_v{version}'
        self.logger.debug(f'{self.property_name} {property_id=}')
        files.write_json(f'output/ruletree/{title}_ruletree.json', ruletree_json)

        with open(f'output/ruletree/{title}_ruletree.json') as f:
            json_object = json.load(f)

        excel_sheet = f'{self.property_name}_v{version}'
        target_data: list[str] = []
        self.find_name_and_openxml(ruletree_json, target_data)
        xml_data: dict[str, str] = {}
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
                                           showlineno: bool = False) -> tuple[str, dict[str, str]]:
        ruletree_json = self.get_property_ruletree(property_id, version)
        title = f'{self.property_name}_v{version}'
        self.logger.debug(f'{self.property_name} {property_id=}')
        Path('output/ruletree').mkdir(parents=True, exist_ok=True)
        files.write_json(f'output/ruletree/{title}_ruletree.json', ruletree_json)

        with open(f'output/ruletree/{title}_ruletree.json') as f:
            json_object = json.load(f)

        excel_sheet = f'{self.property_name}_v{version}'
        target_data: list[str] = []
        self.find_name_and_xml(ruletree_json, target_data)

        # print_json(data=ruletree_json)
        # logger.debug(target_data)
        print()
        xml_data: dict[str, str] = {}
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

    def get_property_advanced_override(self, property_id: int, version: int) -> str:
        _, full_ruletree = super().property_rate_limiting(property_id, version)
        try:
            advancedOverride = full_ruletree['rules']['advancedOverride']
        except:
            pass
        return advancedOverride

    def get_property_path_n_behavior(self, json: dict) -> list:
        navigation = []
        visited_paths = set()

        def traverse_json(json: dict[str, Any], path: str | None = '') -> list[dict[str, Any]]:
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
            return navigation
        traverse_json(json)
        return navigation

    def collect_property_behavior(self, property_name: str, json: dict) -> pd.DataFrame:
        _behavior = self.get_property_path_n_behavior(json)

        flat = pd.json_normalize(_behavior)
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
        behavior['jsonpath'] = behavior.apply(
            lambda row: self.get_jsonpath_match_behavior(
                self.find_jsonpath_behavior(json, behavior=row['name']),
                behavior=row['name'],
                navigation=row['path'],
                data=row['json_or_xml']), axis=1)

        columns = ['property', 'path', 'jsonpath', 'type', 'name', 'json_or_xml', 'custom_behaviorId']
        return behavior[columns]

    def get_property_path_n_criteria(self, json: dict) -> list:
        navigation = []
        visited_paths = set()

        def traverse_json(json: dict, path: str | None = '') -> list[dict]:
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

            return navigation

        traverse_json(json)
        return navigation

    def collect_property_criteria(self, property_name: str, json: dict) -> pd.DataFrame:
        criteria_list = self.get_property_path_n_criteria(json)
        dx = pd.DataFrame()
        if len(criteria_list) > 0:
            flat = pd.json_normalize(criteria_list)
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
            criteria['jsonpath'] = criteria.apply(
                lambda row: self.get_jsonpath_match_criteria(
                    self.find_jsonpath_criteria(json, criterion=row['name']),
                    navigation=row['path'], data=row['json_or_xml']), axis=1)
            columns = ['property', 'path', 'jsonpath', 'type', 'name', 'json_or_xml']
            return criteria[columns]
        return criteria

    def get_property_path_n_criteria_condition(self, json: dict) -> list[dict]:
        navigation = []
        visited_paths = set()

        def traverse_json(json: dict[str, Any], path: str | None = '') -> list[dict[str, Any]]:
            if isinstance(json, dict):
                if 'criteriaMustSatisfy' in json:
                    current_path = f'{path} {json["name"]}'.strip()
                    if current_path not in visited_paths:
                        visited_paths.add(current_path)
                        navigation.append({current_path: json['criteriaMustSatisfy']})

                for k, v in json.items():
                    if k in ['children']:
                        traverse_json(v, f'{path} {json["name"]} {k}')

            elif isinstance(json, list):
                for i, item in enumerate(json):
                    index = i + 1
                    traverse_json(item, f'{path} [{index:>3}] > ')
            return navigation

        traverse_json(json)
        return navigation

    def collect_property_criteria_condition(self, property_name: str, json: dict) -> pd.DataFrame:
        criteria_list = self.get_property_path_n_criteria_condition(json)
        dx = pd.DataFrame()
        if len(criteria_list) > 0:
            flat = pd.json_normalize(criteria_list)
            dx = pd.DataFrame(flat)
            dx = dx.melt(var_name='path', value_name='json')
            dx = dx.dropna(subset=['json'])
            dx['property'] = property_name

        criteria = pd.DataFrame()
        if not dx.empty:
            criteria = dx.explode('json').reset_index(drop=True)
            criteria['type'] = 'criteria_condition'
            criteria['index'] = criteria.groupby(['property', 'path']).cumcount() + 1
            criteria['name'] = 'criteriaMustSatisfy'
        if not criteria.empty:
            criteria['json_or_xml'] = criteria['json']
            criteria['path'] = criteria.apply(lambda row: f"{row['path']} [{str(row['index']):>3}]", axis=1)
            criteria['jsonpath'] = criteria.apply(
                lambda row: self.get_jsonpath_match_criteria_condition(
                    self.find_jsonpath_criteria_condition(json), navigation=row['path']), axis=1)
            columns = ['property', 'path', 'jsonpath', 'type', 'name', 'json_or_xml']
            return criteria[columns]
        return criteria

    def get_product_schema(self, product_id: str, format_version: str | None = 'latest') -> dict:
        status, response = super().get_ruleformat_schema(product_id, format_version)
        if status == 200:
            return response
        else:
            return {}

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

    def check_criteria(self, criteria: list[str], df: pd.DataFrame) -> pd.DataFrame:
        for criterion in criteria:
            if criterion == 'cloudletsOrigin':
                df[criterion] = df.apply(lambda row: self.cloudlets_origin_value(row['ruletree']['rules']), axis=1)
                df[f'{criterion}_count'] = df[criterion].str.len()
                df[criterion] = df[[criterion]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
                self.logger.debug(f'\n{df}')
            if criterion == 'path':
                df[criterion] = df.apply(lambda row: self.path_value(row['ruletree']['rules']), axis=1)
                df[criterion] = df[[criterion]].parallel_apply(lambda x: dataframe.flat_list(x[0]) if len(x[0]) > 0 else '', axis=1)
                df[f'{criterion}_count'] = df[criterion].parallel_apply(lambda x: len(x))
                self.logger.debug(f'\n{df}')
                print(df[[criterion, f'{criterion}_count']])
                df[criterion] = df[[criterion]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        return df

    def cloudlets_origin_value(self, json: dict) -> list[str]:
        origins = []

        def traverse_json(json: dict) -> list[str]:
            if isinstance(json, dict):
                if 'criteria' in json and len(json['criteria']) > 0:
                    for x in json['criteria']:
                        try:
                            if x['name'] == 'cloudletsOrigin':
                                origins.extend(x['options']['originId'])
                        except:
                            pass
                for k, v in json.items():
                    if k in ['children', 'behaviors']:
                        traverse_json(v)
            elif isinstance(json, list):
                for i, item in enumerate(json):
                    index = i + 1
                    traverse_json(item)
            return origins

        traverse_json(json)
        return origins

    def path_value(self, json: dict) -> list[str]:
        origins = []

        def traverse_json(json: dict) -> list[str]:
            if isinstance(json, dict):
                if 'criteria' in json and len(json['criteria']) > 0:
                    for x in json['criteria']:
                        try:
                            if x['name'] == 'path':
                                origins.append(x['options']['values'])
                        except:
                            pass
                for k, v in json.items():
                    if k in ['children', 'behaviors']:
                        traverse_json(v)
            elif isinstance(json, list):
                for i, item in enumerate(json):
                    index = i + 1
                    traverse_json(item)
            return origins

        traverse_json(json)
        return origins

    def check_behavior(self, behaviors: list[str], df: pd.DataFrame, cpcode: CpCodeWrapper) -> DataFrame:
        for behavior in behaviors:
            if behavior == 'origin':
                df[behavior] = df.parallel_apply(lambda row: self.origin_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df[f'{behavior}_count'] = df[behavior].str.len()
                df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            elif behavior == 'setvariable':
                df[behavior] = df.parallel_apply(lambda row: self.setvariable_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df[f'{behavior}_count'] = df[behavior].str.len()
                df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            elif behavior == 'edgeConnect':
                # cloudlets
                df[behavior] = df.parallel_apply(lambda row: self.cloudlets_value(row['propertyName'], row['ruletree']['rules']), axis=1)

            elif behavior == 'siteshield':
                df[behavior] = df.parallel_apply(lambda row: self.siteshield_value(row['propertyName'], row['ruletree']['rules']), axis=1)
                df[behavior] = df[[behavior]].parallel_apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
            elif behavior == 'sureroute':
                df[behavior] = df.parallel_apply(lambda row: self.sureroute_value(row['propertyName'], row['ruletree']['rules']), axis=1)
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
    def behavior_count(property_name: str, rules: dict, target_behavior: str) -> int:
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

    def cpcode_value(self, property_name: str, rules: dict) -> list[str]:
        """
        Cover regular cpcode, visitorPrioritization, netstorage, and Image Manager
        """
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

                elif behavior['name'] == 'imageManager':
                    try:
                        values.append(behavior['options']['cpCodeOriginal']['id'])
                    except:
                        pass
                    try:
                        values.append(behavior['options']['cpCodeTransformed']['id'])
                    except:
                        pass

            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.cpcode_value(property_name, child_rule)
                    values.extend(child_values)
        return list(set(values))

    def custom_behavior_value(self, property_name: str, rules: dict) -> list[str]:
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

    def setvariable_value(self, property_name: str, rules: dict) -> list[str]:
        values = []
        if 'behaviors' in rules.keys() and isinstance(rules['behaviors'], list):
            for behavior in rules['behaviors']:
                if behavior['name'] == 'setVariable':
                    # print_json(data=behavior)
                    if 'variableName' in behavior['options']:
                        variableName = behavior['options']['variableName']

                    if 'variableValue' in behavior['options']:
                        variableValue = behavior['options']['variableValue']
                        x = f'{variableName}:{variableValue}'
                        values.append(x)

                    if 'extractLocation' in behavior['options']:
                        extractLocation = behavior['options']['extractLocation']
                        if 'headerName' in behavior['options']:
                            headerName = behavior['options']['headerName']
                            x = f'{variableName}:{extractLocation}:{headerName}'
                        else:
                            x = f'{variableName}:{extractLocation}'
                        values.append(x)
            if 'children' in rules and isinstance(rules['children'], list):
                for child_rule in rules['children']:
                    child_values = self.setvariable_value(property_name, child_rule)
                    values.extend(child_values)
        return sorted(list(set(values)))

    def origin_value(self, property_name: str, rules: dict) -> list[str]:
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

    def siteshield_value(self, property_name: str, rules: dict) -> list[str]:
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

    def sureroute_value(self, property_name: str, rules: dict) -> list[str]:
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
    def activate_property_version(self, property_id: int, version: int,
                                  network: str,
                                  note: str,
                                  email: list,
                                  review_email: str | None = None) -> int:
        status, response = super().activate_property_version(property_id, version,
                                                             network,
                                                             note,
                                                             email,
                                                             review_email)
        if status == 201:
            try:
                activation_id = int(response.split('?')[0].split('/')[-1])
                return activation_id
            except:
                print_json(data=response)
                self.logger.warning(activation_id)
                return 0
        return -1

    def activation_status(self, property_id: int, activation_id: str, version: int) -> tuple:
        if int(activation_id) == -1:
            return ('', f"{emojis.encode(':x:')}")

        if int(activation_id) != 0 and version > 0:
            status, response = super().activation_status(property_id, activation_id)
            self.logger.debug(f'{activation_id=} {version=} {property_id=} {status=}')
            if response[0]['propertyVersion'] == version:
                status = response[0]['status']
                if status == 'ACTIVE':
                    status = f"{emojis.encode(':white_check_mark:')} {status}"
                elif status == 'PENDING':
                    status = f"{emojis.encode(':hourglass_flowing_sand:')} {status}"

                return (response[0]['network'], status)
        return ('', '')

    # CUSTOM BEHAVIOR
    def list_custom_behaviors(self) -> tuple[int, list[str]]:
        return super().list_custom_behaviors()

    def get_custom_behaviors(self, id: str) -> tuple[int, str]:
        return super().get_custom_behaviors(id)

    # HELPER
    def extract_criteria_json(self, row: pd.Series[Any]) -> str:
        if row['name'] == 'matchAdvanced':
            openXml = row['json']['options']['openXml']
            closeXml = row['json']['options']['closeXml']
            return f'{openXml}{closeXml}'
        else:
            return row['json']['options']

    def extract_behavior_json(self, row: pd.Series[Any]) -> str:
        if row['behavior'] == 'customBehavior':
            return self.get_custom_behaviors(row['custom_behaviorId'])[1]
        if row['behavior'] == 'advanced':
            return row['json']['options']['xml']
        else:
            return row['json']['options']

    def extract_custom_behavior_id(self, row: pd.Series[Any]) -> str:
        if row['behavior'] == 'customBehavior':
            return row['json']['options']['behaviorId']
        else:
            return ''

    def get_jsonpath_match_behavior(self,
                                    result: list[tuple[str, str, str]],
                                    behavior: str,
                                    navigation: str,
                                    data: str) -> Any:
        if len(result) == 1:
            return result[0][0]
        else:
            numbers_inside_brackets = re.findall(r'\[ *(\d+) *\]', navigation)
            navipath_nums = [int(num) for num in numbers_inside_brackets]
            jsonpath_nums = [num - 1 for num in navipath_nums]
            extracted_numbers = [list(map(int, re.findall(r'\d+', item[0]))) for item in result]
            matching_indices = [index for index, numbers in enumerate(extracted_numbers) if numbers == jsonpath_nums]
            matching_paths = [result[index][0] for index in matching_indices]
            self.logger.debug(jsonpath_nums)
            self.logger.debug(extracted_numbers)
            self.logger.debug(matching_paths)
            if len(matching_paths) == 1:
                return matching_paths[0]
            return [x[0] for x in result]

    def find_jsonpath_behavior(self,
                               ruletree: dict[str, Any],
                               behavior: str | None = None,
                               current_path: list[str] = []) -> list[tuple[str, str, str]]:
        result = []

        def traverse(node: dict[str, Any], path: str) -> list[tuple[str, str, str]]:
            if path:
                path += '/'
            path += 'rules'
            behaviors = node.get('behaviors', [])
            for behavior_index, beh in enumerate(behaviors):
                if behavior:
                    if beh['name'] == behavior:
                        result.append((f'{path}/behaviors/{behavior_index}', node['name'], beh['name'], beh['options']))
                else:
                    result.append((f'{path}/behaviors/{behavior_index}', node['name'], beh['name'], beh['options']))

            children = node.get('children', [])
            for child_index, child in enumerate(children):
                traverse(child, f'{path}/children/{child_index}')

            return result

        traverse(ruletree, '')
        return result

    def get_jsonpath_match_criteria(self,
                                    result: list[tuple[str, str, str]],
                                    navigation: str,
                                    data: str) -> Any:
        if len(result) == 1:
            return result[0][0]
        else:
            if '>' not in navigation:
                match_result = [x[0] for x in result if 'children' not in x[0]]
                if len(match_result) == 1:
                    return match_result[0]
            elif 'children' in navigation:
                count_children = navigation.count('children')
                matching_elements = [x[0] for x in result if x[0].count('children') == count_children]
                if len(matching_elements) == 1:
                    return matching_elements[0]
                else:
                    matching_option = [x[0] for x in result if x[2] == data]
                    if len(matching_option) == 1:
                        return matching_option[0]
                    else:
                        self.logger.debug(navigation)
                        self.logger.debug(result)
                        numbers_inside_brackets = re.findall(r'\[ *(\d+) *\]', navigation)
                        navipath_nums = [int(num) for num in numbers_inside_brackets]
                        jsonpath_nums = [num - 1 for num in navipath_nums]
                        extracted_numbers = [list(map(int, re.findall(r'\d+', item[0]))) for item in result]
                        matching_indices = [index for index, numbers in enumerate(extracted_numbers) if numbers == jsonpath_nums]
                        matching_paths = [result[index][0] for index in matching_indices]
                        self.logger.debug(jsonpath_nums)
                        self.logger.debug(extracted_numbers)
                        self.logger.debug(matching_paths)
                        if len(matching_paths) == 1:
                            return matching_paths[0]
            return [x[0] for x in result]

    def find_jsonpath_criteria(self,
                               ruletree: dict[str, Any],
                               criterion: str | None = None,
                               current_path: list[str] = []) -> list[tuple[str, str, str]]:
        result = []

        def traverse(node: dict[str, Any], path: str) -> list[tuple[str, str, str]]:
            if path:
                path += '/'
            path += 'rules'
            criteria = node.get('criteria', [])
            for index, cri in enumerate(criteria):
                if criterion:
                    if cri['name'] == criterion:
                        result.append((f'{path}/criteria/{index}', node['name'], cri['name'], cri['options']))
                else:
                    result.append((f'{path}/criteria/{index}', node['name'], cri['name'], cri['options']))

            children = node.get('children', [])
            for child_index, child in enumerate(children):
                traverse(child, f'{path}/children/{child_index}')

            return result

        traverse(ruletree, '')
        return result

    def get_jsonpath_match_criteria_condition(self,
                                              result: list[tuple[str, str, str, str]],
                                              navigation: str) -> Any:
        if len(result) == 1:
            return result[0][0]
        else:
            for i, item in enumerate(result):
                if i > 0 and result[i][1] in navigation:
                    self.logger.debug(result[i])
                    return result[i][0]
        return [x[0] for x in result]

    def find_jsonpath_criteria_condition(self,
                                         ruletree: dict[str, Any],
                                         current_path: list[str] | None = []) -> list[tuple[str, str, str, str]]:
        result = []

        def traverse(node: dict[str, Any], path: str) -> list[tuple[str, str, str, str]]:
            if path:
                path += '/'
            path += 'rules'
            criteria = node.get('criteriaMustSatisfy', [])
            result.append((path, node['name'], 'criteriaMustSatisfy', criteria))

            children = node.get('children', [])
            for child_index, child in enumerate(children):
                traverse(child, f'{path}/children/{child_index}')
            return result

        traverse(ruletree, '')
        return result


class Node:
    def __init__(self, name: str, value: str, parent: Node | None = None):
        self.name = name
        self.value = value
        self.parent = parent

    def get_path(self) -> str:
        if self.parent is None:
            return self.name
        else:
            return f'{self.parent.get_path()} > {self.name}'


if __name__ == '__main__':
    pass
