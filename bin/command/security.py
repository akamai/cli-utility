from __future__ import annotations

import platform
import re
import subprocess
import sys

import pandas as pd
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import appsec as sec
from tabulate import tabulate
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


def list_configs(args):
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    filepath = f'output/{account}_waf.xlsx' if args.output is None else f'output/{args.output}'

    appsec = sec.AppsecWrapper(account_switch_key=args.account_switch_key)
    network = sec.NetworkListWrapper(account_switch_key=args.account_switch_key)
    bot = sec.BotManagerWrapper(account_switch_key=args.account_switch_key)

    _, resp = appsec.list_waf_configs()
    df = pd.DataFrame(resp)
    columns = ['name', 'id']
    df = df[df['name'] == args.config]
    if df.empty:
        _, resp = appsec.list_waf_configs()
        df = pd.DataFrame(resp)
        columns = ['id', 'name']
        print(tabulate(df[columns], headers=columns, tablefmt='github', numalign='center'))
        sys.exit(logger.error(f"'{args.config}' not found"))
    else:
        config_id = df['id'].values[0]
        status, policy = appsec.get_config_detail(config_id)
        if status == 200:
            df = pd.DataFrame(policy, index=[0])
            version = df['productionVersion'].values[0]

    status, policy = appsec.get_config_version_detail(config_id, version)
    files.write_json('choice.json', policy)

    summary = ['configId', 'configName', 'version', 'versionNotes', 'basedOn',
               'staging.status', 'production.status', 'createdBy']
    df = pd.json_normalize(policy)
    logger.debug(df.columns.values)
    print(tabulate(df[summary], headers=summary, tablefmt='github', numalign='center'))

    sheet = {}

    non_features = ['customBotCategorySequence', 'customClients']
    all_features = list(policy.keys())
    features = sorted(list(set(all_features) - set(summary)))
    logger.info(features)
    for feature in features:
        data = policy[feature]
        if isinstance(data, list):
            logger.info(f'{feature:<30} {len(data):<5}')
        elif isinstance(data, dict):
            logger.warning(f'{feature:<30} {data.keys()}')

    df = pd.DataFrame(policy['selectableHosts'], columns=['selectableHosts'])
    df = df.sort_values(by='selectableHosts').copy()
    df = df.reset_index(drop=True)
    sheet['selectableHosts'] = df

    df = pd.DataFrame(policy['selectedHosts'], columns=['selectedHosts'])
    df = df.sort_values(by='selectedHosts').copy()
    df = df.reset_index(drop=True)
    sheet['selectedHosts'] = df

    sheet['matchTargets'] = bot.process_matchTargets(policy['matchTargets']['websiteTargets'], network)
    sheet['customDefinedBots'] = bot.process_custom_bot(policy['customDefinedBots'], network)
    sheet['ratePolicies'] = bot.process_rate_policies(policy['ratePolicies'], network)
    _, _, sheet['responseActions'] = bot.process_response_actions(policy['responseActions'], network)

    sheet['securityPolicies'] = pd.json_normalize(policy['securityPolicies'])
    sheet['advancedOptions'] = pd.json_normalize(policy['advancedOptions']).T
    sheet['advancedSettings'] = pd.json_normalize(policy['advancedSettings']).T

    print()
    files.write_xlsx(filepath, sheet, adjust_column_width=False, freeze_column=1, show_index=True)
    if platform.system() == 'Darwin':
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
