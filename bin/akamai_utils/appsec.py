from __future__ import annotations

import logging
import sys

import numpy as np
import pandas as pd
from akamai_api.security.appsec import Appsec
from akamai_api.security.botmanager import BotManager
from akamai_api.security.networklist import NetworkList
from rich import print_json
from utils import _logging as lg
from utils import dataframe


class AppsecWrapper(Appsec):
    def __init__(self,
                 account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 cookies: str | None = None,
                 logger: logging.Logger = None):

        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc, cookies=cookies)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_config_detail(self, config_id: int):
        return super().get_config_detail(config_id)

    def get_config_version_detail(self, config_id: int, version: int, exclude: list | None = None):
        return super().get_config_version_detail(config_id, version, exclude)

    def get_config_version_metadata_xml(self, config_name: str, version: int):
        return super().get_config_version_metadata_xml(config_name, version)

    def list_waf_configs(self):
        return super().list_waf_configs()

    def get_policy(self, config_id: int, version: int):
        return super().get_policy(config_id, version)

    def list_custom_rules(self, config_id: int):
        return super().list_custom_rules(config_id)

    def bypass_network_list(self, config_id: int, version: int, policy_id: str):
        return super().bypass_network_list(config_id, version, policy_id)


class NetworkListWrapper(NetworkList):
    def __init__(self,
                 account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_all_network_list(self):
        return super().get_all_network_list()

    def get_network_list(self, ids):
        self.logger.debug(ids)
        if isinstance(ids, str):
            status, result = super().get_network_list(ids)
            if status == 200:
                try:
                    return sorted(result['list'])
                except:
                    self.logger.error(f'{ids:<40} has no IPs')
                    return ['None']
        elif isinstance(ids, list):
            all_ips = []
            for id in ids:
                status, result = super().get_network_list(id)
                if status == 200:
                    try:
                        all_ips.extend(result['list'])
                    except:
                        self.logger.error(f'{id:<40} has no IPs')
                        return ['None']
            return sorted(all_ips)


class BotManagerWrapper(BotManager):
    def __init__(self,
                 account_switch_key: str | None = None,
                 section: str | None = None,
                 edgerc: str | None = None,
                 logger: logging.Logger = None):
        super().__init__(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
        self.account_switch_key = account_switch_key
        self.logger = logger

    def get_all_akamai_bot_catagories(self):
        status, response = super().get_all_akamai_bot_catagories()
        if status == 200:
            try:
                return response['categories']
            except:
                return 0

    def get_akamai_bot_catagory(self, id: str):
        status, response = super().get_akamai_bot_catagory(id)
        if status == 200:
            try:
                return response
            except:
                return 0

    def get_all_custom_bot_catagories(self, config_id: str, version: int):
        status, response = super().get_all_custom_bot_catagories(config_id, version)
        if status == 200:
            try:
                return response['categories']
            except:
                return 0

    def get_custom_bot_catagory(self, config_id: str, version: int, category_id: str):
        status, response = super().get_custom_bot_catagory(config_id, version, category_id)

        if status == 200:
            try:
                return response
            except:
                return 0

    def get_custom_bot_catagory_action(self, config_id: str, version: int, policy_id: str, category_id: str):
        status, response = super().get_custom_bot_catagory_action(config_id, version, policy_id, category_id)

        if status == 200:
            try:
                return response
            except:
                return 0

    def get_custom_bot_catagory_sequence(self, config_id: str, version: int):
        status, response = super().get_custom_bot_catagory_sequence(config_id, version)

        if status == 200:
            try:
                return response['sequence']
            except:
                return 0

    def get_custom_defined_bot(self, config_id: str, version: int, bot_id: str):
        status, response = super().get_custom_defined_bot(config_id, version, bot_id)

        if status == 200:
            try:
                return response
            except:
                return 0

    def process_custom_bot(self, data, network):
        feature = 'customDefinedBots'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                self.logger.critical(f'{feature:<40} no data')
                return pd.DataFrame()

            self.logger.debug(f'{feature:<40} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)
            if 'description' in df.columns:
                del df['description']
            if 'notes' in df.columns:
                del df['notes']
            df['conditions_count'] = df['conditions'].apply(lambda x: len(x) if isinstance(x, list) else 0)

            # df = df[df['botName'] == 'Allow Botify'].copy()

            # extract conditions column
            all_keys = dataframe.extract_keys(df['conditions'].sum())
            columns_to_explode = list(all_keys)
            self.logger.debug(columns_to_explode)

            for key in all_keys:
                df[key] = df['conditions'].apply(lambda x: [d.get(key) for d in x])
            self.logger.debug(f'\n{df[columns_to_explode]}')

            exploded_data = exploded_data = dataframe.explode_cell(df, 'conditions', columns_to_explode)
            exploded_df = pd.DataFrame(exploded_data)
            col_1 = ['categoryId', 'botId', 'botName', 'conditions_no']
            col_2 = ['type', 'name', 'positiveMatch', 'value', 'checkIps', 'valueCase', 'nameWildcard']
            col_2 = [value for value in col_2 if value in columns_to_explode]

            try:
                columns = col_1 + col_2 + ['IPs']
                exploded_df['conditions_no'] = (exploded_df.groupby(['categoryId', 'botId', 'botName']).cumcount() + 1)

                exploded_df['IPs'] = exploded_df.apply(lambda row: network.get_network_list(row['value'][0])
                                                   if row['type'] == 'networkListCondition' else '', axis=1)
                exploded_df['IPs'] = exploded_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                        if row['type'] == 'networkListCondition' else '', axis=1)
                exploded_df['name'] = exploded_df.apply(lambda row: dataframe.split_elements_newline_withcomma(row['name'])
                                                       if isinstance(row['name'], list) else row['name'], axis=1)
                exploded_df['value'] = exploded_df.apply(lambda row: dataframe.split_elements_newline_withcomma(row['value'])
                                                        if row['value'] else '', axis=1)
            except:
                columns = col_1 + col_2

        return exploded_df[columns]

    def process_custom_deny_list(self, data):
        feature = 'customDenyList'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                self.logger.critical(f'{feature:<40} no data')
                return pd.DataFrame()

            self.logger.debug(f'{feature:<40} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)
            df['rule_name'] = df['name']
            del df['name']
            if 'description' in df.columns:
                df['rule_description'] = df['description']
            df['parameters_count'] = df['parameters'].apply(lambda x: len(x) if isinstance(x, list) else 0)
            # extract parameters column
            all_keys = dataframe.extract_keys(df['parameters'].sum())
            parameters_columns = list(all_keys)

            for key in all_keys:
                df[key] = df['parameters'].apply(lambda x: [d.get(key) for d in x])

            exploded_data = exploded_data = dataframe.explode_cell(df, 'parameters', parameters_columns)
            exploded_df = pd.DataFrame(exploded_data)
            original_columns = ['id', 'rule_name']
            if 'description' in df.columns:
                original_columns.append('rule_description')

            exploded_df['parameters_no'] = (exploded_df.groupby(['id', 'rule_name']).cumcount() + 1)
            return exploded_df[original_columns + ['parameters_no'] + ['name', 'displayName', 'value']]

    def process_custom_rules(self, data):
        feature = 'customRules'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                self.logger.critical(f'{feature:<40} no data')
                return pd.DataFrame()
            self.logger.debug(f'{feature:<40} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)
            original_keys = df.columns.tolist()
            if 'conditions' not in original_keys:
                return df
            else:
                original_keys.remove('conditions')
                df['conditions_count'] = df['conditions'].apply(lambda x: len(x) if isinstance(x, list) else 0)

                # extract conditions column
                all_keys = dataframe.extract_keys(df['conditions'].dropna().sum())
                columns_to_explode = list(all_keys)

                if all_keys is None:
                    return pd.DataFrame()
                else:
                    self.logger.debug(columns_to_explode)
                    for key in all_keys:
                        df[key] = df['conditions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

                    col_1 = original_keys
                    col_2 = ['type', 'name', 'value', 'valueWildcard', 'valueCase', 'positiveMatch', 'nameWildcard']
                    col_2 = [value for value in col_2 if value in columns_to_explode]
                    columns = col_1 + ['conditions_no'] + col_2
                    exploded_data = dataframe.explode_cell(df, 'conditions', columns_to_explode)
                    exploded_df = pd.DataFrame(exploded_data)
                    exploded_df['conditions_no'] = (exploded_df.groupby(['id']).cumcount() + 1)
        return exploded_df[columns]

    def process_rate_policies(self, data, network):
        feature = 'ratePolicies'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                self.logger.critical(f'{feature:<40} no data')
                return []

            self.logger.debug(f'{feature:<40} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)

            if 'additionalMatchOptions' not in df.columns.tolist():
                return df
            else:
                original_keys = df.columns.tolist()
                self.logger.debug(original_keys)
                original_keys.remove('additionalMatchOptions')
                original_keys.remove('type')

                df['policy_type'] = df['type']
                df['additionalMatchOptions_count'] = df['additionalMatchOptions'].apply(lambda x: len(x) if isinstance(x, list) else 0)
                all_keys = dataframe.extract_keys(df['additionalMatchOptions'].dropna().sum())
                columns_to_explode = list(all_keys)
                self.logger.debug(columns_to_explode)

                for key in all_keys:
                    df[key] = df['additionalMatchOptions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

                exploded_data = dataframe.explode_cell(df, 'additionalMatchOptions', columns_to_explode)
                exploded_df = pd.DataFrame(exploded_data)
                col_1 = original_keys + ['policy_type']
                columns = col_1 + ['additionalMatchOptions_no'] + columns_to_explode + ['IPs']
                exploded_df['IPs'] = exploded_df.apply(lambda row: network.get_network_list(row['values'])
                                                    if row['type'] == 'NetworkListCondition' else '', axis=1)
                exploded_df['IPs'] = exploded_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                    if row['type'] == 'NetworkListCondition' else '', axis=1)

                exploded_df['additionalMatchOptions_no'] = (exploded_df.groupby(['id']).cumcount() + 1)

                return exploded_df[columns]

    def process_matchTargets(self, data, network):
        feature = 'matchTargets'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                self.logger.critical(f'{feature:<40} no data')
                return pd.DataFrame()

            self.logger.debug(f'{feature:<40} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)
            df['policyId'] = df['securityPolicy.policyId']
            del df['securityPolicy.policyId']

            original_keys = df.columns.tolist()
            self.logger.debug(original_keys)

            if 'bypassNetworkLists' not in original_keys:
                return df
            else:
                df['matchTarget_type'] = df['type']
                df['matchTarget_id'] = df['id']
                original_keys.remove('type')
                original_keys.remove('id')
                original_keys.remove('bypassNetworkLists')
                df['bypassNetworkLists_count'] = df['bypassNetworkLists'].apply(lambda x: len(x) if isinstance(x, list) else 0)

                # extract conditions column
                all_keys = dataframe.extract_keys(df['bypassNetworkLists'].dropna().sum())
                columns_to_explode = list(all_keys)
                self.logger.debug(all_keys)

                for key in all_keys:
                    df[key] = df['bypassNetworkLists'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

                self.logger.debug(f'\n{df[columns_to_explode]}')

                exploded_data = dataframe.explode_cell(df, 'bypassNetworkLists', columns_to_explode)
                exploded_df = pd.DataFrame(exploded_data)

                exploded_df['IPs'] = exploded_df.apply(lambda row: network.get_network_list(row['id'])
                                                            if row['listType'] == 'NL' else '', axis=1)
                exploded_df['IPs'] = exploded_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                            if row['listType'] == 'NL' else '', axis=1)
                self.logger.debug(f'\n{exploded_df}')
                exploded_df = exploded_df.rename(columns={'id': 'bypassNetworkListsId'})

                original_keys.remove('policyId')
                original_keys.remove('sequence')
                col_1 = ['policyId', 'sequence'] + ['matchTarget_type', 'matchTarget_id']
                col_2 = ['bypassNetworkListsId', 'listType', 'name', 'type'] + ['IPs']
                columns = col_1 + ['NetworkListsId_no'] + col_2 + original_keys
                exploded_df['NetworkListsId_no'] = (exploded_df.groupby(['policyId']).cumcount() + 1)

        return exploded_df[columns]

    def process_reputation_profiles(self, data, network):
        feature = 'reputationProfiles'

        df = pd.json_normalize(data)
        df = df.sort_values(by=['context', 'threshold'])
        if 'condition.atomicConditions' not in df.columns.tolist():
            return df
        else:
            df['atomicConditions'] = df['condition.atomicConditions']
            df['context_id'] = df['id']
            del df['condition.atomicConditions']
            del df['id']
            original_keys = df.columns.tolist()

            df['atomicConditions_count'] = df['atomicConditions'].apply(lambda x: len(x) if isinstance(x, list) else 0)
            all_keys = dataframe.extract_keys(df['atomicConditions'].dropna().sum())
            if all_keys:
                for key in all_keys:
                    df[key] = df['atomicConditions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])
                rules_columns = list(all_keys)
                exploded_data = dataframe.explode_cell(df, 'atomicConditions', rules_columns)

                condition_columns = ['className', 'index', 'positiveMatch', 'value', 'valueCase', 'valueWildcard', 'checkIps', 'IPs']
                condition_columns = [value for value in condition_columns if value in all_keys]
                condition_df = pd.DataFrame(exploded_data)

                condition_df['condition.version'] = condition_df['condition.version'].astype(str)

                condition_df['condition.version'] = condition_df['condition.version'].replace({'nan': ' '})

                condition_df['IPs'] = condition_df.apply(lambda row: network.get_network_list(row['value'])
                                                            if row['className'] == 'NetworkListCondition' else '', axis=1)
                condition_df['IPs'] = condition_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                            if row['className'] == 'NetworkListCondition' else '', axis=1)

        return condition_df[original_keys + condition_columns]

    def process_response_actions(self, data, network):
        feature = 'responseActions'

        df = pd.json_normalize(data)
        original_keys = df.columns.tolist()
        self.logger.debug(original_keys)
        if 'conditionalActions' not in original_keys:
            return df, pd.DataFrame(), pd.DataFrame()
        else:
            original_keys.remove('conditionalActions')

            df['conditionalActions_count'] = df['conditionalActions'].apply(lambda x: len(x) if isinstance(x, list) else 0)

            # extract conditions column
            all_keys = dataframe.extract_keys(df['conditionalActions'].dropna().sum())
            columns_to_explode = list(all_keys)

            if all_keys is None:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            else:
                for key in all_keys:
                    df[key] = df['conditionalActions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

                self.logger.debug(f'\n{df[columns_to_explode]}')

                exploded_data = dataframe.explode_cell(df, 'conditionalActions', columns_to_explode)
                exploded_df = pd.DataFrame(exploded_data)
                conditionalActions_count = exploded_df['conditionalActions_count'].sum()

                if conditionalActions_count == 0:
                    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                else:
                    col_1 = original_keys
                    col_2 = ['conditionalActions_count', 'conditionalActions'] + ['actionId', 'actionName', 'defaultAction', 'conditionalActionRules', 'description']
                    if 'description' not in all_keys:
                        col_2.remove('description')
                        self.logger.debug(col_2)
                    columns = col_1 + col_2 + ['conditionalActionRules_count']

                    exploded_df['conditionalActionRules_count'] = exploded_df['conditionalActionRules'].apply(lambda x: len(x) if isinstance(x, list) else 0)
                    all_keys = dataframe.extract_keys(exploded_df['conditionalActionRules'].dropna().sum())
                    columns_to_explode = list(all_keys)
                    self.logger.debug(columns_to_explode)
                    for key in all_keys:
                        exploded_df[key] = exploded_df['conditionalActionRules'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

                    exploded_data = dataframe.explode_cell(exploded_df, 'conditionalActionRules', columns_to_explode)
                    new_df = pd.DataFrame(exploded_data)

                    all_keys = dataframe.extract_keys(new_df['conditions'].dropna().sum())
                    columns_to_explode = list(all_keys)

                    for key in all_keys:
                        new_df[key] = new_df['conditions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

                    exploded_data = dataframe.explode_cell(new_df, 'conditions', columns_to_explode)
                    conditions_df = pd.DataFrame(exploded_data)

                    col_1 = ['challengeActions', 'customDenyActions', 'serveAlternateActions', 'challengeInjectionRules.injectJavaScript', 'challengeInterceptionRules.interceptAllRequests']
                    col_2 = ['actionId', 'defaultAction', 'actionName', 'percentageOfTraffic', 'action']
                    col_3 = ['checkIps', 'positiveMatch', 'type', 'value', 'host', 'valueCase', 'nameWildcard', 'valueWildcard']
                    col_3 = [value for value in col_3 if value in columns_to_explode]
                    cols = col_1 + col_2 + col_3 + ['IPs']
                    conditions_df['IPs'] = conditions_df.apply(lambda row: network.get_network_list(row['value'])
                                                            if row['type'] == 'networkListCondition' else '', axis=1)
                    conditions_df['IPs'] = conditions_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                            if row['type'] == 'networkListCondition' else '', axis=1)
                    self.logger.debug(conditions_df[cols])
                    return exploded_df[columns], new_df, conditions_df[cols]

    def process_rulesets(self, data):
        feature = 'rulesets'
        # extract rules column
        df = pd.json_normalize(data)
        df['ruleset_id'] = df['id']
        del df['id']
        original_keys = df.columns.tolist()
        if 'attackGroups' in df.columns.tolist():
            original_keys.remove('attackGroups')
        if 'rules' not in df.columns.tolist():
            return None, None
        else:
            original_keys.remove('rules')
        df['rules_count'] = df['rules'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        all_keys = dataframe.extract_keys(df['rules'].dropna().sum())
        if all_keys:
            for key in all_keys:
                df[key] = df['rules'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])
            rules_columns = list(all_keys)
            exploded_data = dataframe.explode_cell(df, 'rules', rules_columns)
            rules_df = pd.DataFrame(exploded_data)
            original_keys.remove('ruleset_id')
            rules_columns = ['attackGroups', 'id', 'inspectRequestBody', 'inspectResponseBody', 'outdated', 'ruleVersion', 'score', 'tag', 'title']

            rules_df = rules_df[['ruleset_id'] + original_keys + rules_columns]
            rules_df = rules_df.sort_values(by='attackGroups')
        # extract attackGroups column
        df = pd.json_normalize(data)
        df['ruleset_id'] = df['id']
        original_keys = df.columns.tolist()

        if 'id' in df.columns.tolist():
            original_keys.remove('id')
            del df['id']

        '''
        attack_group_df = pd.json_normalize(df['attackGroups'].explode().reset_index(drop=True))
        attack_group_df['order'] = attack_group_df.index + 1
        '''
        if 'attackGroups' in df.columns.tolist():
            original_keys.remove('attackGroups')
            df['attackGroups_count'] = df['attackGroups'].apply(lambda x: len(x) if isinstance(x, list) else 0)

        all_keys = dataframe.extract_keys(df['attackGroups'].dropna().sum())
        # col = ['attackGroups', 'attackGroups_count']
        # self.logger.info(df[col])
        if all_keys:
            for key in all_keys:
                df[key] = df['attackGroups'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])
            group_columns = list(all_keys)

        exploded_data = dataframe.explode_cell(df, 'attackGroups', group_columns)
        attack_group_df = pd.DataFrame(exploded_data)
        group_columns = ['group', 'groupName', 'threshold']
        original_keys.remove('ruleset_id')
        attack_group_df = attack_group_df[['ruleset_id'] + original_keys + group_columns]

        return rules_df, attack_group_df


if __name__ == '__main__':
    pass
