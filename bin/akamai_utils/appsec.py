from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from akamai_api.security.appsec import Appsec
from akamai_api.security.botmanager import BotManager
from akamai_api.security.networklist import NetworkList
from rich import print_json
from utils import _logging as lg
from utils import dataframe

logger = lg.setup_logger()


class AppsecWrapper(Appsec):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None,
                 cookies: str | None = None):
        super().__init__()
        self.account_switch_key = account_switch_key

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
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None):
        super().__init__()
        self.account_switch_key = account_switch_key

    def get_all_network_list(self):
        return super().get_all_network_list()

    def get_network_list(self, ids):
        logger.debug(ids)
        if isinstance(ids, str):
            status, result = super().get_network_list(ids)
            if status == 200:
                try:
                    return sorted(result['list'])
                except:
                    logger.error(f'{ids} has no IPs')
                    return 0
        elif isinstance(ids, list):
            all_ips = []
            for id in ids:
                status, result = super().get_network_list(id)
                if status == 200:
                    try:
                        all_ips.extend(result['list'])
                    except:
                        logger.error(f'{id} has no IPs')
            return sorted(all_ips)


class BotManagerWrapper(BotManager):
    def __init__(self, account_switch_key: str | None = None,
                 section: str | None = None):
        super().__init__()
        self.account_switch_key = account_switch_key

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
                logger.critical(f'{feature} no data')
                sys.exit(logger.error(data[0].keys()))

            logger.debug(f'{feature:<30} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)
            del df['description']
            del df['notes']
            df['conditions_count'] = df['conditions'].apply(lambda x: len(x) if isinstance(x, list) else 0)

            # df = df[df['botName'] == 'Allow Botify'].copy()

            # extract conditions column
            all_keys = dataframe.extract_keys(df['conditions'].sum())
            columns_to_explode = list(all_keys)
            logger.debug(columns_to_explode)

            for key in all_keys:
                df[key] = df['conditions'].apply(lambda x: [d.get(key) for d in x])
            logger.debug(f'\n{df[columns_to_explode]}')

            exploded_data = exploded_data = dataframe.explode_cell(df, 'conditions', columns_to_explode)
            exploded_df = pd.DataFrame(exploded_data)
            col_1 = ['botId', 'botName', 'categoryId', 'conditions', 'conditions_count']
            col_2 = ['type', 'name', 'positiveMatch', 'value', 'checkIps', 'valueCase', 'nameWildcard']
            columns = col_1 + col_2 + ['IPs']

            exploded_df['IPs'] = exploded_df.apply(lambda row: network.get_network_list(row['value'][0])
                                                   if row['type'] == 'networkListCondition' else '', axis=1)
            exploded_df['IPs'] = exploded_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                    if row['type'] == 'networkListCondition' else '', axis=1)
            exploded_df['name'] = exploded_df.apply(lambda row: dataframe.split_elements_newline_withcomma(row['name'])
                                                    if row['name'] else '', axis=1)
            exploded_df['value'] = exploded_df.apply(lambda row: dataframe.split_elements_newline_withcomma(row['value'])
                                                     if row['value'] else '', axis=1)

        return exploded_df[columns]

    def process_rate_policies(self, data, network):
        feature = 'ratePolicies'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                logger.critical(f'{feature} no data')
                sys.exit(logger.error(data[0].keys()))

            logger.debug(f'{feature:<30} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)
            original_keys = df.columns.tolist()
            logger.debug(original_keys)
            original_keys.remove('additionalMatchOptions')

            df['original_type'] = df['type']
            df['additionalMatchOptions_count'] = df['additionalMatchOptions'].apply(lambda x: len(x) if isinstance(x, list) else 0)

            # extract conditions column
            all_keys = dataframe.extract_keys(df['additionalMatchOptions'].sum())
            columns_to_explode = list(all_keys)
            logger.debug(columns_to_explode)

            for key in all_keys:
                df[key] = df['additionalMatchOptions'].apply(lambda x: [d.get(key) for d in x])

            exploded_data = dataframe.explode_cell(df, 'additionalMatchOptions', columns_to_explode)
            exploded_df = pd.DataFrame(exploded_data)
            logger.debug(f'\n{exploded_df}')

            col_1 = original_keys + ['original_type']
            col_2 = ['additionalMatchOptions', 'additionalMatchOptions_count'] + columns_to_explode
            columns = col_1 + col_2 + ['IPs']
            exploded_df['IPs'] = exploded_df.apply(lambda row: network.get_network_list(row['values'])
                                                   if row['type'] == 'NetworkListCondition' else '', axis=1)
            exploded_df['IPs'] = exploded_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                   if row['type'] == 'NetworkListCondition' else '', axis=1)
        return exploded_df[columns]

    def process_matchTargets(self, data, network):
        feature = 'matchTargets'

        if isinstance(data, list):
            try:
                columns = data[0].keys()
            except:
                logger.critical(f'{feature} no data')
                sys.exit(logger.error(data[0].keys()))

            logger.debug(f'{feature:<30} {len(columns):<5} {columns}')
            df = pd.json_normalize(data)

            original_keys = df.columns.tolist()
            logger.debug(original_keys)
            df['original_type'] = df['type']
            df['original_id'] = df['id']
            original_keys.remove('type')
            original_keys.remove('id')
            original_keys.remove('bypassNetworkLists')

            df['bypassNetworkLists_count'] = df['bypassNetworkLists'].apply(lambda x: len(x) if isinstance(x, list) else 0)

            # extract conditions column
            all_keys = dataframe.extract_keys(df['bypassNetworkLists'].dropna().sum())
            columns_to_explode = list(all_keys)
            logger.debug(all_keys)

            for key in all_keys:
                df[key] = df['bypassNetworkLists'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

            logger.debug(f'\n{df[columns_to_explode]}')

            exploded_data = dataframe.explode_cell(df, 'bypassNetworkLists', columns_to_explode)
            exploded_df = pd.DataFrame(exploded_data)
            logger.debug(f'\n{exploded_df}')

            col_1 = original_keys + ['original_type', 'original_id']
            col_2 = ['bypassNetworkLists', 'bypassNetworkLists_count'] + ['id', 'listType', 'name', 'type']
            columns = col_1 + col_2

        return exploded_df[columns]

    def process_response_actions(self, data, network):
        feature = 'responseActions'

        df = pd.json_normalize(data)
        original_keys = df.columns.tolist()
        logger.info(original_keys)
        original_keys.remove('conditionalActions')

        df['conditionalActions_count'] = df['conditionalActions'].apply(lambda x: len(x) if isinstance(x, list) else 0)

        # extract conditions column
        all_keys = dataframe.extract_keys(df['conditionalActions'].dropna().sum())
        columns_to_explode = list(all_keys)
        logger.debug(columns_to_explode)

        for key in all_keys:
            df[key] = df['conditionalActions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

        logger.debug(f'\n{df[columns_to_explode]}')

        exploded_data = dataframe.explode_cell(df, 'conditionalActions', columns_to_explode)
        exploded_df = pd.DataFrame(exploded_data)
        logger.debug(f'\n{exploded_df}')

        col_1 = original_keys
        col_2 = ['conditionalActions_count', 'conditionalActions'] + \
         ['actionId', 'actionName', 'defaultAction', 'conditionalActionRules', 'description']
        # ['challengeActions', 'conditionalActions', 'customDenyActions', 'serveAlternateActions', 'challengeInjectionRules.injectJavaScript', 'challengeInterceptionRules.interceptAllRequests']
        columns = col_1 + col_2 + ['conditionalActionRules_count']
        exploded_df['conditionalActionRules_count'] = exploded_df['conditionalActionRules'].apply(lambda x: len(x) if isinstance(x, list) else 0)

        all_keys = dataframe.extract_keys(exploded_df['conditionalActionRules'].dropna().sum())
        columns_to_explode = list(all_keys)
        logger.info(columns_to_explode)
        for key in all_keys:
            exploded_df[key] = exploded_df['conditionalActionRules'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

        exploded_data = dataframe.explode_cell(exploded_df, 'conditionalActionRules', columns_to_explode)
        new_df = pd.DataFrame(exploded_data)

        all_keys = dataframe.extract_keys(new_df['conditions'].dropna().sum())
        columns_to_explode = list(all_keys)
        logger.info(columns_to_explode)
        for key in all_keys:
            new_df[key] = new_df['conditions'].apply(lambda x: [d.get(key) for d in x] if isinstance(x, list) else [])

        exploded_data = dataframe.explode_cell(new_df, 'conditions', columns_to_explode)
        conditions_df = pd.DataFrame(exploded_data)

        col_1 = ['challengeActions', 'customDenyActions', 'serveAlternateActions', 'challengeInjectionRules.injectJavaScript', 'challengeInterceptionRules.interceptAllRequests']
        col_2 = ['actionId', 'defaultAction', 'actionName', 'percentageOfTraffic', 'action']
        col_3 = ['checkIps', 'positiveMatch', 'type', 'value', 'host', 'valueCase', 'nameWildcard', 'valueWildcard']
        cols = col_1 + col_2 + col_3 + ['IPs']
        conditions_df['IPs'] = conditions_df.apply(lambda row: network.get_network_list(row['value'])
                                                   if row['type'] == 'networkListCondition' else '', axis=1)
        conditions_df['IPs'] = conditions_df.apply(lambda row: dataframe.split_elements_newline(row['IPs'])
                                                   if row['type'] == 'NetworkListCondition' else '', axis=1)

        return exploded_df[columns], new_df, conditions_df[cols]


if __name__ == '__main__':
    pass
