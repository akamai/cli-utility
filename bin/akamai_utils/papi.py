from __future__ import annotations

import pandas as pd
from akamai_api.papi import Papi
from utils import _logging as lg
from utils import dataframe

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
        logger.debug(f'{group_id=} {contract_id=}')
        # TODO groups with multiple contracts
        '''
        if group_id in [14777, 14786, 49036, 33877, 50009, 37733, 61564] \
            or group_id in [14805, 47146]:
            properties = []
        else:
        '''
        try:
            properties = self.get_propertyname_per_group(group_id, contract_id)
        except:
            0
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
            logger.warning(f'Collecting properties for {group_id=} {contract_id=}')
            properties = self.get_propertyname_per_group(group_id, contract_id)
            property_count[group_id] = len(properties)
            property_df = pd.DataFrame(properties)
            df_list.append(property_df)
        return pd.concat(df_list), property_count

    def normarlize_rows(self, df):

        mc = df[df['multiple_contracts'] > 1].copy()
        mcx = pd.DataFrame()
        if not mc.empty:
            mcx = dataframe.explode(mc, 'groupId', 'contractIds', new_column='contractId')
            mcx['groupId'] = mcx['groupId'].astype('Int64')
            mcx['groupName'] = mcx[['groupId']].parallel_apply(lambda x: self.get_group_name(*x), axis=1)
            mcx['parentGroupId'] = mcx[['groupId']].parallel_apply(lambda x: self.get_parent_group_id(*x), axis=1)
            mcx['total_properties'] = mcx[['groupId', 'contractId']].parallel_apply(lambda x: self.get_properties_count_in_group(*x), axis=1)
            mcx['properties'] = mcx[['groupId', 'contractId']].parallel_apply(lambda x: self.get_propertyname_per_group(*x), axis=1)
            mcx = mcx.reset_index(drop=True)
            columns = ['groupName', 'groupId', 'parentGroupId', 'contractId', 'total_properties']
            mcx = mcx[columns]
            logger.debug(f'normarlize_rows\n{mcx}')
        return mcx

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
            return [property_id]
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
    def account_summary(self) -> pd.DataFrame:
        _, top_groups = self.get_top_groups()
        if not top_groups.empty:
            sheets = {}
            property_df, property_size = self.get_properties_in_group()
            if 'note' in property_df.columns:
                property_df.drop(columns=['note'], axis=1, inplace=True)
            top_groups['total_properties'] = top_groups['groupId'].swifter.apply(lambda x: property_size[x])
            top_groups['multiple_contracts'] = top_groups.contractIds.apply(lambda x: len(x))
            sheets['parent_count'] = top_groups
            logger.debug(f'\n{top_groups}')

            # filter rows with multiple contracts
            multiple_contract = self.normarlize_rows(top_groups)

            single_contract = top_groups[top_groups['multiple_contracts'] == 1].copy()
            single_contract['contractId'] = single_contract['contractIds'].apply(lambda col: col[0])
            single_contract = single_contract.drop(['order', 'multiple_contracts', 'contractIds'], axis=1)
            columns = ['groupName', 'groupId', 'parentGroupId', 'contractId', 'total_properties']
            single_contract = single_contract[columns]
            logger.debug(f'\n{single_contract}')

            if not multiple_contract.empty:
                df = pd.concat([single_contract, multiple_contract], axis=0)
            else:
                df = top_groups
                df = df.rename({'contractIds': 'contractId'}, axis=1)

            df['subfolder'] = df[['groupId']].parallel_apply(lambda x: self.get_child_groups(*x), axis=1)
            # issue with TC East, if remove comment, break Walmart
            # df['contractId'] = df[['contractId']].apply(lambda col: col.str[0])
            logger.debug(f'\n{df}')

            # order displayed on UI
            df['groupname'] = df['groupName'].str.lower()
            df.sort_values(by=['parentGroupId', 'groupname', 'total_properties'], inplace=True, na_position='first')
            del df['groupname']
            df = df.reset_index(drop=True)
            df.index.name = 'portal_display_order'
            columns = ['groupName', 'groupId', 'parentGroupId', 'contractId', 'total_properties', 'subfolder']
            account = df[columns]
            sheets['summary'] = account
            logger.debug(f'\n{account}')
        return account

    def normarlize_groups_hierarchy(self) -> dict:
        account_df = self.account_summary()
        ch = account_df[account_df['subfolder'].map(len) > 0]
        ch = ch.reset_index(drop=True)
        columns = ['groupName', 'groupId', 'parentGroupId', 'contractId', 'total_properties']
        ch = ch[columns]
        logger.debug(f'GROUPs with subfolders\n{ch}')

        all_groups = []
        temp_dict = {}
        chunklen = 1
        groups = ch.groupId.unique().tolist()
        for parent_group_id in groups:
            children = self.get_child_group_id(parent_group_id)
            logger.debug(children)
            temp_dict = {k: parent_group_id for k in children if len(children) > 0}
            temp_list = list(temp_dict.items())
            extract_list = [dict(temp_list[i:i + chunklen]) for i in range(0, len(temp_list), chunklen)]
            if extract_list:
                all_groups.extend(extract_list)
            while len(children) > 0:
                groups = children
                for parent_group_id in groups:
                    children = self.get_child_group_id(parent_group_id)
                    logger.debug(children)
                    temp_dict = {k: parent_group_id for k in children if len(children) > 0}
                    temp_list = list(temp_dict.items())
                    extract_list = [dict(temp_list[i:i + chunklen]) for i in range(0, len(temp_list), chunklen)]
                    if extract_list:
                        all_groups.extend(extract_list)

        # merge a list of dicts into a single dict
        all_groups_dict = {k: v for d in all_groups for k, v in d.items()}
        return all_groups_dict

    def account_statistic(self):
        columns = ['groupName', 'groupId', 'parentGroupId', 'contractId', 'total_properties']
        all_groups = self.normarlize_groups_hierarchy()
        df = pd.DataFrame.from_dict(all_groups, orient='index', columns=['parentGroupId'])
        df.index.name = 'groupId'
        df = df.reset_index()

        df['groupName'] = df[['groupId']].parallel_apply(lambda x: self.get_group_name(*x), axis=1)
        df['contractIds'] = df[['groupId']].parallel_apply(lambda x: self.get_group_contract_id(*x), axis=1)
        df['multiple_contracts'] = df.contractIds.apply(lambda x: len(x))
        logger.debug(df)

        multiple_contract = self.normarlize_rows(df)
        if not multiple_contract.empty:
            multiple_contract = multiple_contract[multiple_contract['total_properties'] > 0].copy()
            multiple_contract = multiple_contract.reset_index(drop=True)
            # 1:M group:contracts
            logger.debug(f'1:M group:contracts\n{multiple_contract}')

        single_contract = df[df['multiple_contracts'] == 1].copy()
        single_contract['contractId'] = single_contract['contractIds'].apply(lambda col: col[0])
        single_contract = single_contract.drop(['multiple_contracts', 'contractIds'], axis=1)
        single_contract['total_properties'] = single_contract[['groupId', 'contractId']].parallel_apply(lambda x: self.get_properties_count_in_group(*x), axis=1)
        single_contract = single_contract[columns]
        single_contract = single_contract.reset_index(drop=True)
        # 1:1 group:contract
        logger.debug(f'1:1 group:contract\n{single_contract}')

        if not multiple_contract.empty:
            childs = pd.concat([single_contract, multiple_contract], axis=0)
        else:
            childs = single_contract
            childs = childs.rename({'contractIds': 'contractId'}, axis=1)
            childs = childs.sort_values(by=['parentGroupId', 'groupId'], na_position='first')
            childs = childs.reset_index(drop=True)
            childs.index.name = 'portal_display_order'
            # childs['groupId'] = childs['groupId'].astype(str)
            childs = childs[columns]
            childs = childs.reset_index()
            logger.debug(f'\n{childs}')

        account_df = self.account_summary()
        account_df.index.name = 'portal_display_order'
        # account_df['groupId'] = account_df['groupId'].astype(str)
        account_df = account_df[columns]
        account_df = account_df.reset_index()
        logger.debug(f'\n{account_df}')

        stat_df = pd.concat([account_df, childs], axis=0)
        stat_df['groupId'] = stat_df['groupId'].astype(str)
        stat_df = stat_df.reset_index(drop=True)
        # Final Groups Stats
        logger.debug(f'Final Groups Stats\n{stat_df}')
        return stat_df

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
