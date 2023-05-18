from __future__ import annotations

import pandas as pd
from akamai_api.papi import Papi
from utils._logging import setup_logger

logger = setup_logger()


class PapiWrapper(Papi):
    def __init__(self, account_switch_key: str | None = None):
        super().__init__()
        self.account_switch_key = account_switch_key

    def get_contracts(self):
        contracts = super().get_contracts()
        df = pd.DataFrame(contracts)
        df.sort_values(by=['contractId'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        logger.info(f'\n{df}')
        sorted_contracts = sorted([contract['contractId'] for contract in contracts])
        logger.info(f'{sorted_contracts=}')
        return contracts

    def get_parent_groups(self) -> tuple:
        groups = super().get_groups()
        df = pd.DataFrame(groups)
        df = df[df['parentGroupId'].isnull()]  # group with empty parent
        df['groupname'] = df['groupName'].str.lower()
        df.sort_values(by=['parentGroupId', 'groupname'], inplace=True, na_position='first')
        df = df.reset_index(drop=True)
        df['order'] = df.index
        df = df.drop(['groupname'], axis=1)

        groups = df['groupId'].unique()
        logger.debug(f'Parent\n{df}')
        logger.debug(groups)
        return groups, df

    def get_groups(self) -> tuple:
        groups = super().get_groups()
        df = pd.DataFrame(groups)
        df = df[~df['parentGroupId'].isnull()]  # group with parent
        df['groupname'] = df['groupName'].str.lower()
        df.sort_values(by=['parentGroupId', 'groupId'], inplace=True, na_position='first')
        df.reset_index(inplace=True, drop=True)
        df.drop(['groupname'], axis=1, inplace=True)
        groups = df['groupId'].unique()
        logger.debug(f'Child\n{df}')
        logger.debug(groups)
        return groups, df

    def get_properties_count_in_group(self, group_id: int, contract_id: str) -> int:
        properties = self.get_properties_per_group(group_id, contract_id)
        return len(properties)

    def get_properties_in_group(self, group_id: int | None = None, contract_id: str | None = None) -> tuple:
        df_list = []
        property_count = {}
        if not group_id and not contract_id:
            parent_groups, df = self.get_parent_groups()
            for group_id in parent_groups:
                contracts = df[df['groupId'] == group_id]['contractIds'].item()
                group_name = df[df['groupId'] == group_id]['groupName'].item()
                if len(contracts) > 1:
                    count = 0
                    for i, contract_id in enumerate(contracts, 1):
                        logger.debug(f'{group_name} {group_id} {contract_id}')
                        properties = self.get_properties_per_group(group_id, contract_id)
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
                    properties = self.get_properties_per_group(group_id, contracts[0])
                    property_count[group_id] = len(properties)
                    if not bool(properties):
                        logger.debug(f'{group_name} {group_id} {contracts[0]} {properties} no property')
                    else:
                        property_df = pd.DataFrame(properties)
                        df_list.append(property_df)
        else:
            logger.warning(f'Collecting properties for {group_id=} {contract_id=}')
            properties = self.get_properties_per_group(group_id, contract_id)
            property_count = len(properties)
            property_df = pd.DataFrame(properties)
            df_list.append(property_df)
        return pd.concat(df_list), property_count

    # def get_properties_in_group(self, group_id: int, contract_id: str) -> list:
    #    return super().get_properties_per_group(group_id, contract_id)

    def get_correct_contract(self, parent_group_id: str, contract_id: list):
        if parent_group_id == '14777':
            return contract_id[0]
        elif parent_group_id == '14805':
            return contract_id[1]
        else:
            return contract_id[0]

    def get_properties_ruletree_digest(self, property_id: int, version: int):
        return super().get_properties_ruletree_digest(property_id, version)

    def get_properties_detail(self, property_id: int, version: int):
        x = super().get_properties_detail(property_id, version)
        return x[0]['updatedDate']

    def get_property_hostnames(self, property_id: int):
        data = super().get_property_hostnames(property_id)
        df = pd.DataFrame(data)
        hostnames = df['cnameFrom'].unique().tolist()
        return hostnames

    def get_behavior(self, rule_dict: dict, behavior: str) -> tuple:
        print()
        logger.warning(behavior)
        rule_dict = rule_dict['definitions']['catalog']['behaviors']
        data = {key: value for (key, value) in rule_dict.items() if key == behavior}
        options = data[behavior]['properties']['options']['properties']
        logger.debug(data)
        logger.debug(options)
        return data, options


if __name__ == '__main__':
    pass
