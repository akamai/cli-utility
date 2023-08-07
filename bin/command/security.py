from __future__ import annotations

import platform
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import appsec as sec
from akamai_utils import papi as p
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import dataframe
from utils import files
from yaspin import yaspin


def list_config(args, account_folder, logger):
    account_switch_key, section, edgerc = args.account_switch_key, args.section, args.edgerc
    appsec = sec.AppsecWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
    network = sec.NetworkListWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
    bot = sec.BotManagerWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)

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

        if 'description' in columns:
            print(tabulate(df[columns], headers=columns, tablefmt='grid', numalign='center', showindex=True, maxcolwidths=50))
        else:
            print(tabulate(df[columns], headers=columns, tablefmt='github', numalign='center', showindex=True))

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

            if status != 200:
                logger.error(f"{row['configId']}  {status} {row['productionVersion']} {row['latestVersion']}")
                break

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


def audit_hostname(args, account_folder, logger):
    account_switch_key, section, edgerc = args.account_switch_key, args.section, args.edgerc
    Path(account_folder).mkdir(parents=True, exist_ok=True)

    appsec = sec.AppsecWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
    _, resp = appsec.list_waf_configs()
    df = pd.DataFrame(resp)
    df = df.rename(columns={'id': 'configId', 'name': 'configName'})
    df = df.sort_values(by=['groupId', 'configName'], na_position='first')
    df = df.reset_index(drop=True)
    df['groupId'] = df['groupId'].apply(lambda x: str(int(x)) if pd.notna(x) else x)
    df = df.fillna('')
    df['stagingVersion'] = df['stagingVersion'].replace('', 0)
    df['stagingVersion'] = df['stagingVersion'].astype('Int64')
    df['productionVersion'] = df['productionVersion'].replace('', 0)
    df['productionVersion'] = df['productionVersion'].astype('Int64')
    df['productionHostnames_count'] = df['productionHostnames'].str.len()

    columns = ['groupId', 'configName', 'configId', 'productionHostnames_count', 'stagingVersion', 'productionVersion', 'latestVersion']

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
    df['productionHostnames_sorted'] = df['productionHostnames'].apply(sorted)
    del df['productionHostnames']
    df = df.rename(columns={'productionHostnames_sorted': 'productionHostnames'})
    temp_columns = ['contractId', 'groupId', 'configId', 'configName', 'productionHostnames_count', 'stagingVersion', 'productionVersion', 'latestVersion']

    print()
    logger.warning('Individual Security Configuration')
    print(tabulate(df[temp_columns], headers=temp_columns, showindex=True,
                   tablefmt='github', numalign='center'))

    columns = temp_columns + ['productionHostnames']
    waf_security_df = df[columns].copy()
    waf_security_df['productionHostnames'] = waf_security_df[['productionHostnames']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
    columns = ['contractId', 'groupId', 'configId', 'configName', 'stagingVersion', 'productionVersion', 'latestVersion', 'productionHostnames', 'productionHostnames_count']
    waf_security_df = waf_security_df[columns]

    # Group by 'groupId' and apply the aggregation functions
    agg_dict = {
        'configId': lambda x: x.tolist(),
        'configName': lambda x: x.tolist(),
        'productionHostnames': lambda x: sorted(sum(x, []))  # Concatenate lists
        }

    security_config_by_group = df.groupby(['contractId', 'groupId']).agg(agg_dict).reset_index()
    security_config_by_group['productionHostnames_count'] = security_config_by_group['productionHostnames'].str.len()
    security_config_by_group = security_config_by_group.sort_values(by='groupId')

    print()
    logger.warning('Security Configuration by GroupId')
    temp_columns = ['contractId', 'groupId', 'configId', 'configName', 'productionHostnames_count']
    if len(security_config_by_group.index) < len(waf_security_df.index):
        print(tabulate(security_config_by_group[temp_columns], headers=temp_columns, showindex=True,
                       numalign='center', tablefmt='grid', maxcolwidths=50))

    # get property from group
    print()
    logger.warning('Collecting hostnames from delivery configs from the same groups')
    with yaspin():
        papi = p.PapiWrapper(account_switch_key=args.account_switch_key, logger=logger)
        allgroups_df, columns = papi.account_group_summary()

    properties = df.query("groupId != ''")
    security_groups = properties['groupId'].unique().tolist()
    allgroups_df['groupId'] = allgroups_df['groupId'].astype(str)
    group_df = allgroups_df[allgroups_df['groupId'].isin(security_groups)].copy()
    group_df = group_df.reset_index(drop=True)
    columns.remove('groupName')
    columns = ['contractId', 'groupId', 'parentGroupId', 'group_structure', 'propertyCount']
    print(tabulate(group_df[columns], headers=columns, showindex=True, tablefmt='github'))
    print()

    account_properties = papi.property_summary(group_df)
    if len(account_properties) > 0:
        delivery = pd.concat(account_properties, axis=0)

    # only look for property activated on Staging only
    staging = delivery[delivery['productionVersion'].apply(lambda x: pd.isna(x) or x == '')].copy()
    agg_dict = {
        'groupId': lambda x: list(set(x.tolist())),
        'propertyName': lambda x: x.tolist(),
        'hostname': sum  # Concatenate lists
        }
    staging = staging.groupby('groupId').agg(agg_dict)
    staging = staging.reset_index(drop=True)
    staging['groupId'] = staging[['groupId']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
    staging['hostname_count'] = staging['hostname'].str.len()
    columns = ['groupId', 'propertyName', 'hostname', 'hostname_count']
    staging = staging[columns]

    delivery['hostname'] = delivery[['hostname']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
    del delivery['propertyURL']
    delivery = delivery.rename(columns={'url': 'propertyName (hyperlink)'})

    if not staging.empty:
        print()
        logger.warning('Delivery configs activated on staging only')
        print(tabulate(staging, headers=columns, showindex=True, tablefmt='grid', maxcolwidths=50))

    # Merge the dataframes on 'groupId'
    '''
    new_value = ['a', 'A', 'Y', 'Z']
    group_id = '26333'
    mask = delivery['groupId'] == group_id
    logger.debug(delivery['hostname'])
    delivery.loc[mask, 'hostname'] = delivery.loc[mask, 'hostname'].apply(lambda x: x + new_value)
    logger.info(f"\n{delivery['hostname']}")
    '''

    merged_df = pd.merge(security_config_by_group, staging, on='groupId', how='left')
    diff_df = merged_df.dropna(subset=['hostname'])
    diff_df = diff_df[diff_df['hostname'].apply(len) > 0]  # remove rows where 'hostname' is an empty list
    if not diff_df.empty:
        # Convert NaN values to empty lists
        diff_df['productionHostnames'] = diff_df['productionHostnames'].fillna('').apply(lambda x: x if isinstance(x, list) else [])
        diff_df['hostname'] = diff_df['hostname'].fillna('').apply(lambda x: x if isinstance(x, list) else [])
        diff_df['host_not_in_waf'] = diff_df.apply(lambda row: [hostname for hostname in row['hostname'] if hostname not in row['productionHostnames']], axis=1)
        diff_df['host_not_in_waf'] = diff_df[['host_not_in_waf']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        diff_df['productionHostnames'] = diff_df[['productionHostnames']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        diff_df['delivery_hostname'] = diff_df[['hostname']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        diff_df['configId'] = diff_df[['configId']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        diff_df['configName'] = diff_df[['configName']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        columns = ['host_not_in_waf', 'delivery_hostname', 'productionHostnames', 'groupId', 'configId', 'configName', 'productionHostnames_count']
    if diff_df.empty:
        logger.info('all hostnames from delivery configs are in security configs')
    else:
        # cleanup data for excel
        waf_security_df['configId'] = waf_security_df['configId'].astype(str)

        security_config_by_group['configId'] = security_config_by_group[['configId']].apply(
            lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        security_config_by_group['configName'] = security_config_by_group[['configName']].apply(
            lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)

        staging = staging.sort_values(by='groupId')
        staging['propertyName'] = staging[['propertyName']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        staging['hostname'] = staging[['hostname']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
        staging = staging.reset_index(drop=True)

        sheet = {}
        sheet['compare'] = diff_df[columns]
        sheet['security_by_groupId'] = security_config_by_group
        sheet['properties_by_groupId'] = staging
        sheet['original_security_configs'] = waf_security_df
        sheet['delivery_properties_detail'] = delivery
        sheet['delivery_summary'] = group_df

        filepath = f'{account_folder}/hostname_audit.xlsx' if args.output is None else f'output/{args.output}'
        files.write_xlsx(filepath, sheet, adjust_column_width=True, freeze_column=3)
        files.open_excel_application(filepath, show=True, df=diff_df[columns])
