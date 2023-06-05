from __future__ import annotations

import re

import numpy as np
import pandas as pd
from akamai_api.papi import Papi
from pandarallel import pandarallel
from utils import _logging as lg
from utils import dataframe
from utils import files

logger = lg.setup_logger()


class PapiWrapper(Papi):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.account_switch_key = account_switch_key

    def get_contracts(self):
        contracts = super().get_contracts()
        df = pd.DataFrame(contracts)
        df.sort_values(by=['contractId'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        sorted_contracts = sorted([contract['contractId'] for contract in contracts])
        logger.info(f'{sorted_contracts=}')
        return contracts

    def get_edgehostnames(self, contract_id: str, group_id: int):
        return super().get_edgehostnames(contract_id, group_id)

    # GROUPS
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
            logger.debug(groups)
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
            logger.debug(f'Group Detail\n{df}')
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
            logger.debug(f'Group Detail\n{df}')
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
        logger.debug(f'{group_id=} {contract_id=}')
        properties_json = super().get_propertyname_per_group(group_id, contract_id)
        property_df = pd.DataFrame(properties_json)
        properties = []
        if not property_df.empty:
            properties = property_df['propertyName'].values.tolist()
        return properties

    def get_properties_detail_per_group(self, group_id: int, contract_id: str) -> pd.DataFrame:
        logger.debug(f'{group_id=} {contract_id=}')
        properties_json = super().get_propertyname_per_group(group_id, contract_id)
        property_df = pd.DataFrame(properties_json)
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
                        logger.debug(f'{group_name} {group_id} {contract_id}')
                        properties = super().get_propertyname_per_group(group_id, contract_id)
                        count += len(properties)

                        if not bool(properties):
                            logger.debug(f'{group_name} {group_id} {contracts[0]} {properties} no property')
                        else:
                            logger.debug(f'Collecting properties for {group_name:<50} {group_id:<10} {contract_id:<10}')
                            property_df = pd.DataFrame(properties)
                            df_list.append(property_df)
                    property_count[group_id] = count
                elif len(contracts) == 1:
                    logger.debug(f'Collecting properties for {group_name:<50} {group_id:<10} {contracts[0]:<10}')
                    properties = self.get_propertyname_per_group(group_id, contracts[0])
                    property_count[group_id] = len(properties)
                    if not bool(properties):
                        logger.debug(f'{group_name} {group_id} {contracts[0]} {properties} no property')
                    else:
                        property_df = pd.DataFrame(properties)
                        df_list.append(property_df)
        else:
            logger.debug(f'Collecting properties for {group_id=} {contract_id=}')
            properties = self.get_propertyname_per_group(group_id, contract_id)
            property_count[group_id] = len(properties)
            property_df = pd.DataFrame(properties)
            df_list.append(property_df)
        return pd.concat(df_list), property_count

    # PROPERTIES
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

    def get_property_version_detail(self, property_id: int, version: int, dict_key: str):
        '''
        df['updatedDate'] = df[['propertyId', 'latestVersion']].parallel_apply(lambda x, y: papi.get_properties_detail(x,y, 'updatedDate'), axis=1)
        '''
        detail = super().get_property_version_detail(property_id, version)
        try:
            return detail[0][dict_key]
        except:
            return property_id

    # WHOLE ACCOUNT
    def account_group_summary(self) -> tuple:
        _, all_groups = self.get_all_groups()
        df = self.create_groups_dataframe(all_groups)

        df['name'] = df['L0'].str.lower()  # this column will be used for sorting later
        df['groupId'] = df['groupId'].astype(int)  # API has groupId has interger
        df['parentGroupId'] = pd.to_numeric(df['parentGroupId'], errors='coerce').fillna(0)  # API has groupId has interger
        df['parentGroupId'] = df['parentGroupId'].astype(int)

        df = df.sort_values(by=['name', 'parentGroupId', 'L1', 'groupId'])
        df = df.drop(['level'], axis=1)
        df = df.fillna('')
        df = df.reset_index(drop=True)

        pandarallel.initialize(progress_bar=False)
        df['account'] = self.account_switch_key
        df['propertyCount'] = df.parallel_apply(lambda row: self.get_properties_count(row), axis=1)
        df['contractId'] = df.parallel_apply(lambda row: self.get_valid_contract(row), axis=1)

        columns = df.columns.tolist()
        levels = [col for col in columns if col.startswith('L')]  # get hierachy
        columns = ['path'] + levels + ['account', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount', 'name']
        stag_df = df[columns].copy()

        # Split rows some groups/folders have multiple contracts
        stag_df = stag_df.apply(lambda row: dataframe.split_rows(row, column_name='contractId'), axis=1)
        stag_df = pd.concat(list(stag_df), ignore_index=True)

        df = stag_df[columns].copy()
        df = df.reset_index(drop=True)
        df['propertyCount'] = df.parallel_apply(lambda row: self.get_properties_count(row), axis=1)

        allgroups_df = df.copy()

        allgroups_df = allgroups_df.reset_index(drop=True)

        columns = ['index_1', 'updated_path', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount'] + levels

        allgroups_df['updated_path'] = allgroups_df.apply(lambda row: self.update_path(allgroups_df, row, column_name='path'), axis=1)
        allgroups_df['index_1'] = allgroups_df.index
        allgroups_df = allgroups_df[columns].copy()
        first_non_empty = allgroups_df.replace('', np.nan).ffill(axis=1).iloc[:, -1]
        allgroups_df['excel_sheet'] = ''
        allgroups_df['excel_sheet'] = np.where(first_non_empty == '', allgroups_df['L0'], first_non_empty)

        pattern = r'[A-Z0-9]-[A-Z0-9]+'
        allgroups_df['excel_sheet'] = allgroups_df['excel_sheet'].apply(lambda x: re.sub(pattern, '', x))
        allgroups_df['excel_sheet'] = allgroups_df['excel_sheet'].apply(lambda x: files.prepare_excel_sheetname(x))
        allgroups_df = allgroups_df.sort_values(by='excel_sheet')
        allgroups_df = allgroups_df.reset_index(drop=True)
        columns = columns + ['excel_sheet']

        allgroups_df = allgroups_df[columns].copy()
        allgroups_df['sheet'] = ''
        allgroups_df = files.update_sheet_column(allgroups_df)
        columns = ['index_1', 'updated_path'] + ['groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount', 'sheet']

        allgroups_df = allgroups_df[columns].copy()
        allgroups_df = allgroups_df.sort_values(by='index_1')
        allgroups_df = allgroups_df.reset_index(drop=True)

        columns = ['updated_path', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount', 'sheet']
        allgroups_df = allgroups_df[columns].copy()
        return allgroups_df, columns

    def property_summary(self, df: pd.DataFrame) -> list:
        account_properties = []
        print()
        for index, row in df.iterrows():
            logger.warning(f"{index:<5} {row['groupName']:<50} {row['propertyCount']}")
            properties = self.get_properties_detail_per_group(row['groupId'], row['contractId'])
            if not properties.empty:
                properties['propertyId'] = properties['propertyId'].astype('Int64')
                properties['groupName'] = row['groupName']  # add group name

                logger.debug(' Collecting hostname')
                properties['hostname'] = properties[['propertyId']].parallel_apply(lambda x: self.get_property_hostnames(*x), axis=1)
                properties['hostname_count'] = properties['hostname'].str.len()
                # show one hostname per list and remove list syntax
                properties['hostname'] = properties[['hostname']].parallel_apply(lambda x: ',\n'.join(x.iloc[0]) if not x.empty else '', axis=1)

                logger.debug(' Collecting updatedDate')
                properties['updatedDate'] = properties.apply(lambda row: self.get_property_version_detail(row['propertyId'], row['latestVersion'], 'updatedDate'), axis=1)

                logger.debug(' Collecting ruleFormat')
                properties['ruleFormat'] = properties.apply(lambda row: self.get_property_version_detail(row['propertyId'], row['latestVersion'], 'ruleFormat'), axis=1)

                logger.debug(' Collecting property url')
                properties['propertyURL'] = properties.apply(lambda row: self.property_url(row['assetId'], row['groupId']), axis=1)

                account_properties.append(properties)
        return account_properties

    # RULETREE
    def get_properties_ruletree_digest(self, property_id: int, version: int):
        '''
        sample
        df['ruleFormat'] = df[['propertyId', 'latestVersion']].parallel_apply(lambda x: papi.get_properties_ruletree_digest(*x), axis=1)
        '''
        return super().get_properties_ruletree_digest(property_id, version)

    def get_property_limit(self, property_id: int, version: int):
        limit, _ = super().property_rate_limiting(property_id, version)
        return limit

    def get_property_ruletree(self, property_id: int, version: int):
        _, ruletree = super().property_rate_limiting(property_id, version)
        return ruletree

    # BEHAVIORS
    def get_behavior(self, rule_dict: dict, behavior: str) -> dict:
        rule_dict = rule_dict['definitions']['catalog']['behaviors']
        data = {key: value for (key, value) in rule_dict.items() if behavior in key}
        # options = data[behavior]['properties']['options']['properties']
        return data

    # ACTIVATION
    def activate_property_version(self, property_id: int, version: int, network: str, note: str, emails: list):
        _, response = super().activate_property_version(property_id, version, network, note, emails)
        return response

    def activation_status(self, property_id: int, activation_id: int, version: int):

        if activation_id > 0 and version > 0:
            status, response = super().activation_status(property_id, activation_id)
            logger.debug(f'{activation_id=} {version=} {property_id=} {status}')
            df = pd.DataFrame(response)
            logger.debug(f'BEFORE\n{df}')
            df = df[df['propertyVersion'] == version].copy()
            logger.debug(f'FILTERED\n{df}')
            return df.status.values[0]
        else:
            return ' '


if __name__ == '__main__':
    pass
