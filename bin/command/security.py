from __future__ import annotations

import platform
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import appsec as sec
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


def list_config(args):
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = iam.show_account_summary(account)
    account_folder = f'output/security/{account}'
    Path(account_folder).mkdir(parents=True, exist_ok=True)
    appsec = sec.AppsecWrapper(account_switch_key=args.account_switch_key)
    network = sec.NetworkListWrapper(account_switch_key=args.account_switch_key)
    bot = sec.BotManagerWrapper(account_switch_key=args.account_switch_key)

    _, resp = appsec.list_waf_configs()
    df = pd.DataFrame(resp)
    df = df.rename(columns={'id': 'configId', 'name': 'configName'})
    df['groupId'] = df['groupId'].apply(lambda x: str(int(x)) if pd.notna(x) else x)
    df = df.fillna('')
    df['stagingVersion'] = df['stagingVersion'].replace('', 0)
    df['stagingVersion'] = df['stagingVersion'].astype('Int64')
    df['productionVersion'] = df['productionVersion'].replace('', 0)
    df['productionVersion'] = df['productionVersion'].astype('Int64')

    columns = ['configName', 'configId', 'groupId', 'stagingVersion', 'productionVersion', 'latestVersion', 'targetProduct', 'fileType']
    if 'description' in df.columns:
        columns.append('description')

    all_configs = df['configName'].values.tolist()
    good_configs = []
    if args.config:
        good_configs = list(set(all_configs).intersection(set(args.config)))
        good_df = df[df['configName'].isin(good_configs)].copy()
        good_df = good_df.reset_index(drop=True)

    if len(good_configs) == 0:
        if args.group_id:
            df = df[df['groupId'].isin(args.group_id)].copy()
            df = df.reset_index(drop=True)
            all_configs = df['configName'].values.tolist()

        print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center', showindex=True, maxcolwidths=50))

        modified_list = ["'" + word + "'" for word in all_configs]
        all_configs_str = ' '.join(modified_list)
        logger.warning(f'--config {all_configs_str}')
    else:
        if args.group_id:
            good_df = good_df[good_df['groupId'].isin(args.group_id)].copy()
            good_df = good_df.reset_index(drop=True)
        if not good_df.empty:
            print(tabulate(good_df[columns], headers=columns, tablefmt='simple', numalign='center', showindex=True, maxcolwidths=50))
        else:
            logger.info('not found any security configuration based on the search criteria')

    if args.config:
        notfound_configs = [x for x in args.config if x not in all_configs]
        if notfound_configs:
            logger.error(f'{notfound_configs} not found.  You need to provide an exact spelling')

    all_files = []
    all_excels = []
    counter = 0
    if args.config is None:
        logger.critical('Please provide at least one configName using --config')
    else:
        for _, row in good_df.iterrows():

            if row['productionVersion'] == 0:
                status, policy = appsec.get_config_version_detail(row['configId'], row['latestVersion'])
            else:
                status, policy = appsec.get_config_version_detail(row['configId'], row['productionVersion'])
            policy_name = policy['configName'].replace(' ', '')
            policy_name = policy_name.replace('/', '')
            filepath = f'{account_folder}/{policy_name}.xlsx' if args.output is None else f'output/{args.output}'
            files.write_json(f'{account_folder}/{policy_name}.json', policy)

            print()
            counter += 1
            logger.warning(f"config no. {counter:<4}'{row['configName']}'")
            summary = ['configId', 'configName', 'version', 'basedOn',
                    'staging.status', 'production.status', 'createdBy', 'versionNotes']
            if 'versionNotes' not in policy.keys():
                summary.remove('versionNotes')
            if 'basedOn' not in policy.keys():
                summary.remove('basedOn')

            df = pd.json_normalize(policy)
            logger.debug(df.columns.values)
            print(tabulate(df[summary], headers=summary, tablefmt='simple', numalign='center', showindex=False, maxcolwidths=50))

            sheet = {}
            advanced = []
            try:
                mdf = pd.json_normalize(policy['siem'])
                mdf.index = pd.Index(['value'])
                mdf = mdf.T
                mdf = mdf.reset_index()
                mdf = mdf.rename(columns={'index': 'title'})
                mdf['key'] = 'siem'
                advanced.append(mdf)
            except:
                pass

            tdf = pd.json_normalize(policy['advancedOptions'])
            tdf.index = pd.Index(['value'])
            tdf = tdf.T
            tdf = tdf.reset_index()
            tdf = tdf.rename(columns={'index': 'title'})
            tdf['key'] = 'advancedOptions'
            advanced.append(tdf)

            try:
                sdf = pd.json_normalize(policy['advancedSettings'])
                sdf.index = pd.Index(['value'])
                sdf = sdf.T
                sdf = sdf.reset_index()
                sdf = sdf.rename(columns={'index': 'title'})
                sdf['key'] = 'advancedSettings'
                advanced.append(sdf)
            except:
                pass

            df = pd.concat(advanced, axis=0)
            sheet['advanced'] = df[['key', 'title', 'value']]

            all_hosts = []
            selectableHosts_df = pd.DataFrame(policy['selectableHosts'], columns=['selectableHosts'])
            selectableHosts_df = selectableHosts_df.sort_values(by='selectableHosts').copy()
            selectableHosts_df = selectableHosts_df.reset_index(drop=True)
            all_hosts.append(selectableHosts_df)
            selectedHosts_df = pd.DataFrame(policy['selectedHosts'], columns=['selectedHosts'])
            selectedHosts_df = selectedHosts_df.sort_values(by='selectedHosts').copy()
            selectedHosts_df = selectedHosts_df.reset_index(drop=True)
            all_hosts.append(selectedHosts_df)

            try:
                errorHosts_df = pd.DataFrame(policy['errorHosts'])
                errorHosts_df = errorHosts_df.rename(columns={'hostname': 'errorHosts'})
                errorHosts_df = errorHosts_df.sort_values(by='errorHosts')
                errorHosts_df = errorHosts_df['errorHosts']
                all_hosts.append(errorHosts_df)
            except:
                pass

            sheet['hosts'] = pd.concat(all_hosts, axis=1)
            sheet['securityPolicies'] = pd.json_normalize(policy['securityPolicies'])

            try:
                sheet['matchTargets'] = bot.process_matchTargets(policy['matchTargets']['websiteTargets'], network)
            except:
                feature = 'matchTargets'
                logger.critical(f'{feature:<40} no data')

            try:
                df = bot.process_custom_bot(policy['customDefinedBots'], network)
                if not df.empty:
                    sheet['customDefinedBots'] = df
            except:
                feature = 'customDefinedBots'
                logger.critical(f'{feature:<40} no data')

            df = bot.process_custom_deny_list(policy['customDenyList'])
            if not df.empty:
                sheet['customDenyList'] = df

            df = bot.process_custom_rules(policy['customRules'])
            if not df.empty:
                sheet['customRules'] = df

            rate_policy = bot.process_rate_policies(policy['ratePolicies'], network)
            if len(rate_policy) > 0:
                sheet['ratePolicies'] = rate_policy

            _, _, response_action_df = bot.process_response_actions(policy['responseActions'], network)
            if not response_action_df.empty:
                sheet['responseActions'] = response_action_df
            else:
                feature = 'responseActions'
                logger.critical(f'{feature:<40} no data')

            if len(policy['rulesets']) > 0:
                sheet['rulesets'], sheet['rulesets_attackgroup'] = bot.process_rulesets(policy['rulesets'])
            else:
                feature = 'rulesets'
                logger.critical(f'{feature:<40} no data')

            try:
                sheet['reputationProfiles'] = bot.process_reputation_profiles(policy['reputationProfiles'], network)
            except:
                feature = 'reputationProfiles'
                logger.critical(f'{feature:<40} no data')

            all_excels.append(sheet)
            all_files.append(filepath)

    if all_files:
        print()
        for filepath, excel in zip(all_files, all_excels):
            files.write_xlsx(filepath, excel, adjust_column_width=False, freeze_column=3)
            if platform.system() == 'Darwin' and args.no_show is False:
                subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
