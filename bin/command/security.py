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


def list_configs(args):
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    print()
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    account_folder = f'output/security/{account}'
    Path(account_folder).mkdir(parents=True, exist_ok=True)

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
        columns = ['name', 'id']
        print(tabulate(df[columns], headers=columns, tablefmt='github', numalign='center'))

        if args.config is None:
            sys.exit(logger.error('Please enter at least one --config'))
        else:
            sys.exit(logger.error(f"'{args.config}' not found.  You need to provide an exact spelling"))
    else:
        config_id = df['id'].values[0]
        status, policy = appsec.get_config_detail(config_id)
        if status == 200:
            df = pd.DataFrame(policy, index=[0])
            try:
                version = df['productionVersion'].values[0]
            except:
                version = df['latestVersion'].values[0]
                logger.warning('no active production version')
                logger.debug(f'\n{df}')

    status, policy = appsec.get_config_version_detail(config_id, version)
    policy_name = policy['configName'].replace(' ', '')
    filepath = f'{account_folder}/{policy_name}.xlsx' if args.output is None else f'output/{args.output}'
    files.write_json(f'{account_folder}/{policy_name}.json', policy)

    summary = ['configId', 'configName', 'version', 'versionNotes', 'basedOn',
               'staging.status', 'production.status', 'createdBy']
    if 'versionNotes' not in policy.keys():
        summary.remove('versionNotes')
    if 'basedOn' not in policy.keys():
        summary.remove('basedOn')

    df = pd.json_normalize(policy)
    logger.debug(df.columns.values)
    print(tabulate(df[summary], headers=summary, tablefmt='github', numalign='center', showindex=False))

    all_features = list(policy.keys())
    non_features = ['staging', 'production', 'customBotCategorySequence', 'customClients', 'siem']
    used = ['advancedOptions', 'advancedSettings', 'customRules', 'securityPolicies', 'rulesets'
            'customDefinedBots', 'customBotCategories',
            'selectableHosts', 'selectedHosts', 'matchTargets',
            'ratePolicies', 'responseActions']
    features = sorted(list(set(all_features) - set(summary) - set(used) - set(non_features)))
    print()

    '''
    logger.warning(features)
    for feature in features:
        data = policy[feature]
        if isinstance(data, list):
            logger.info(f'{feature:<30} {len(data):<5}')
        elif isinstance(data, dict):
            logger.warning(f'{feature:<30} {data.keys()}')
    '''
    sheet = {}
    advanced = []

    mdf = pd.json_normalize(policy['siem'])
    mdf.index = pd.Index(['value'])
    mdf = mdf.T
    mdf = mdf.reset_index()
    mdf = mdf.rename(columns={'index': 'title'})
    mdf['key'] = 'siem'
    advanced.append(mdf)

    tdf = pd.json_normalize(policy['advancedOptions'])
    tdf.index = pd.Index(['value'])
    tdf = tdf.T
    tdf = tdf.reset_index()
    tdf = tdf.rename(columns={'index': 'title'})
    tdf['key'] = 'advancedOptions'
    advanced.append(tdf)

    sdf = pd.json_normalize(policy['advancedSettings'])
    sdf.index = pd.Index(['value'])
    sdf = sdf.T
    sdf = sdf.reset_index()
    sdf = sdf.rename(columns={'index': 'title'})
    sdf['key'] = 'advancedSettings'
    advanced.append(sdf)

    df = pd.concat(advanced, axis=0)
    sheet['advanced'] = df[['key', 'title', 'value']]

    selectableHosts_df = pd.DataFrame(policy['selectableHosts'], columns=['selectableHosts'])
    selectableHosts_df = selectableHosts_df.sort_values(by='selectableHosts').copy()
    selectableHosts_df = selectableHosts_df.reset_index(drop=True)
    selectedHosts_df = pd.DataFrame(policy['selectedHosts'], columns=['selectedHosts'])
    selectedHosts_df = selectedHosts_df.sort_values(by='selectedHosts').copy()
    selectedHosts_df = selectedHosts_df.reset_index(drop=True)
    sheet['hosts'] = pd.concat([selectableHosts_df, selectedHosts_df], axis=1)
    sheet['securityPolicies'] = pd.json_normalize(policy['securityPolicies'])
    try:
        sheet['matchTargets'] = bot.process_matchTargets(policy['matchTargets']['websiteTargets'], network)
    except:
        feature = 'matchTargets'
        logger.critical(f'{feature:<30} no data')

    df = bot.process_custom_bot(policy['customDefinedBots'], network)
    if not df.empty:
        sheet['customDefinedBots'] = df

    df = bot.process_custom_rules(policy['customRules'], network)
    if not df.empty:
        sheet['customRules'] = df

    rate_policy = bot.process_rate_policies(policy['ratePolicies'], network)
    if len(rate_policy) > 0:
        sheet['ratePolicies'] = rate_policy

    _, _, response_action_df = bot.process_response_actions(policy['responseActions'], network)
    if not response_action_df.empty:
        sheet['responseActions'] = response_action_df

    if len(policy['rulesets']) > 0:
        sheet['rulesets'], sheet['rulesets_attackgroup'] = bot.process_rulesets(policy['rulesets'])
    else:
        feature = 'rulesets'
        logger.critical(f'{feature:<30} no data')

    sheet['reputationProfiles'] = bot.process_reputation_profiles(policy['reputationProfiles'], network)

    print()
    files.write_xlsx(filepath, sheet, adjust_column_width=False, freeze_column=2)
    if platform.system() == 'Darwin' and args.no_show is False:
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
