from __future__ import annotations

import copy
import json
import os
import platform
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from subprocess import STDOUT

import numpy as np
import pandas as pd
import swifter
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
from pandarallel import pandarallel
from rich import print_json
from rich.console import Console
from rich.syntax import Syntax
from tabulate import tabulate
from utils import _logging as lg
from utils import files

logger = lg.setup_logger()
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', None)


def main(args):
    '''
    python bin/akamai-utility.py -a 1-1IY5Z delivery-config --show --group-id 14803 163889 162428 90428 14805 82695
    2173 properties 90 minutes
     800 properties 30 minutes
    '''
    if args.group_id and args.property:
        sys.exit(logger.error('Please use either --group-id or --property, not both'))
    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    filepath = f'output/{account}.xlsx' if args.output is None else f'output/{args.output}'
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    sheet = {}
    if args.property:
        all_properties = []
        for property in args.property:
            logger.warning(f'Lookup property {property}')
            status, resp = papi.search_property_by_name(property)
            # print_json(data=resp)
            if status != 200:
                logger.info(f'property {property:<50} not found')
                break
            else:
                logger.critical(f'{papi.group_id} {papi.contract_id} {papi.property_id}')
                stg, prd = papi.property_version(resp)
                all_properties.append((papi.account_id, papi.contract_id, papi.group_id, property, papi.property_id, stg, prd))

        properties_df = pd.DataFrame(all_properties, columns=['accountId', 'contractId', 'groupId', 'propertyName', 'propertyId', 'stagingVersion', 'productionVersion'])
        properties_df['groupName'] = properties_df['groupId'].apply(lambda x: papi.get_group_name(x))
        properties_df['latestVersion'] = properties_df['propertyId'].apply(lambda x: papi.get_property_version_latest(x)['latestVersion'])
        properties_df['assetId'] = properties_df['propertyId'].apply(lambda x: papi.get_property_version_latest(x)['assetId'])

        logger.debug(' Collecting hostname')
        properties_df['hostname'] = properties_df[['propertyId']].apply(lambda x: papi.get_property_hostnames(*x), axis=1)
        properties_df['hostname_count'] = properties_df['hostname'].str.len()
        # show one hostname per list and remove list syntax
        properties_df['hostname'] = properties_df[['hostname']].apply(lambda x: ',\n'.join(x.iloc[0]) if not x.empty else '', axis=1)

        logger.debug(' Collecting updatedDate')
        properties_df['updatedDate'] = properties_df.apply(lambda row: papi.get_property_version_detail(row['propertyId'], row['latestVersion'], 'updatedDate'), axis=1)

        logger.debug(' Collecting productId')
        properties_df['productId'] = properties_df.apply(lambda row: papi.get_property_version_detail(
            row['propertyId'], int(row['productionVersion']) if pd.notnull(row['productionVersion']) else row['latestVersion'], 'productId'), axis=1)

        logger.debug(' Collecting ruleFormat')
        properties_df['ruleFormat'] = properties_df.apply(lambda row: papi.get_property_version_detail(
            row['propertyId'], int(row['productionVersion']) if pd.notnull(row['productionVersion']) else row['latestVersion'], 'ruleFormat'), axis=1)

        logger.debug(' Collecting property url')
        properties_df['propertyURL'] = properties_df.apply(lambda row: papi.property_url(row['assetId'], row['groupId']), axis=1)
        properties_df['url'] = properties_df.apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['propertyURL'], row['propertyName']), axis=1)

        del properties_df['propertyName']  # drop original column
        properties_df = properties_df.rename(columns={'url': 'propertyName'})  # show column with hyperlink instead
        properties_df = properties_df.rename(columns={'groupName_url': 'groupName'})  # show column with hyperlink instead
        properties_df = properties_df.sort_values(by=['groupName', 'propertyName'])

        # properties.loc[pd.notnull(properties['cpcode_unique_value']) & (properties['cpcode_unique_value'] == ''), 'cpcode'] = '0'

        columns = ['accountId', 'groupId', 'groupName', 'propertyName', 'propertyId',
                    'latestVersion', 'stagingVersion', 'productionVersion',
                    'productId', 'ruleFormat', 'hostname_count', 'hostname']
        properties_df['propertyId'] = properties_df['propertyId'].astype(str)
        properties_df = properties_df[columns].copy()
        properties_df = properties_df.reset_index(drop=True)
        sheet['properties'] = properties_df

        properties_df['ruletree'] = properties_df.apply(
                        lambda row: papi.get_property_ruletree(row['propertyId'], int(row['productionVersion'])
                                                            if pd.notnull(row['productionVersion']) else row['latestVersion']), axis=1)
        logger.info(f'\n{properties_df}')
    else:
        # build group structure as displayed on control.akamai.com
        allgroups_df, columns = papi.account_group_summary()
        allgroups_df['groupId'] = allgroups_df['groupId'].astype(str)  # change groupId to str before load into excel

        if args.group_id:
            groups = args.group_id
            group_df = allgroups_df[allgroups_df['groupId'].isin(groups)].copy()
            group_df = group_df.reset_index(drop=True)
        else:
            group_df = allgroups_df[allgroups_df['propertyCount'] > 0].copy()
            group_df = group_df.reset_index(drop=True)

        if not group_df.empty:
            print()
            print(tabulate(group_df, headers=columns, showindex=True, tablefmt='github'))

        # warning for large account
        if not args.group_id:
            print()
            if group_df.shape[0] > 0:
                logger.warning(f'total groups {allgroups_df.shape[0]}, only {group_df.shape[0]} groups have properties.')
            total = allgroups_df['propertyCount'].sum()
            if total > 100:
                logger.warning(f'This account has {total:,} properties, please be patient')
                print()
                logger.critical(' 200 properties take ~  7 minutes')
                logger.critical(' 800 properties take ~ 30 minutes')
                logger.critical('2200 properties take ~ 80 minutes')
                logger.critical('please consider using --group-id to reduce total properties')

        # collect properties detail for all groups
        properties_df = pd.DataFrame()
        if group_df.empty:
            logger.info('no property to collect.')
        else:
            print()
            total = group_df['propertyCount'].sum()
            if total == 0:
                logger.info('no property to collect.')
            else:
                logger.warning('collecting properties ...')
                account_properties = papi.property_summary(group_df)
                if len(account_properties) > 0:
                    properties_df = pd.concat(account_properties, axis=0)

                    properties_df['ruletree'] = properties_df.parallel_apply(
                        lambda row: papi.get_property_ruletree(row['propertyId'], int(row['productionVersion'])
                                                            if pd.notnull(row['productionVersion']) else row['latestVersion']), axis=1)

                    if args.behavior:
                        original_behaviors = [x.lower() for x in args.behavior]
                        updated_behaviors = copy.deepcopy(original_behaviors)

                        if 'cpcode' in original_behaviors:
                            updated_behaviors.remove('cpcode')

                        for behavior in updated_behaviors:
                            properties_df[behavior] = properties_df.parallel_apply(
                                lambda row: papi.behavior_count(row['propertyName'],
                                row['ruletree']['rules'], behavior), axis=1)

                        if 'cpcode' in original_behaviors:
                            properties_df['cpcodes'] = properties_df.apply(
                                    lambda row: papi.cpcode_value(row['propertyName'],
                                    row['ruletree']['rules']) if row['productionVersion'] else 0, axis=1)
                            properties_df['cpcode_unique_value'] = properties_df['cpcodes'].parallel_apply(lambda x: list(set(x)) if isinstance(x, list) else [])
                            properties_df['cpcode_unique_value'] = properties_df['cpcode_unique_value'].parallel_apply(lambda x: sorted(x))
                            properties_df['cpcode_count'] = properties_df['cpcode_unique_value'].parallel_apply(lambda x: len(x))
                            # display one value per line
                            properties_df['cpcode_unique_value'] = properties_df[['cpcode_unique_value']].parallel_apply(
                                lambda x: ',\n'.join(map(str, x.iloc[0])) if isinstance(x.iloc[0], (list, tuple)) and x[0] != '0' else '', axis=1)

                    del properties_df['propertyName']  # drop original column

                    properties_df = properties_df.rename(columns={'url': 'propertyName'})  # show column with hyperlink instead
                    properties_df = properties_df.rename(columns={'groupName_url': 'groupName'})  # show column with hyperlink instead
                    properties_df = properties_df.sort_values(by=['groupName', 'propertyName'])

                    # properties_df.loc[pd.notnull(properties_df['cpcode_unique_value']) & (properties_df['cpcode_unique_value'] == ''), 'cpcode'] = '0'

                    columns = ['accountId', 'groupId', 'groupName', 'propertyName', 'propertyId',
                               'latestVersion', 'stagingVersion', 'productionVersion',
                               'productId', 'ruleFormat', 'hostname_count', 'hostname']
                    if args.behavior:
                        columns.extend(sorted(updated_behaviors))
                        if 'cpcode' in original_behaviors:
                            columns.extend(['cpcode_unique_value', 'cpcode_count'])

                    properties_df['propertyId'] = properties_df['propertyId'].astype(str)
                    properties_df = properties_df[columns].copy()
                    properties_df = properties_df.reset_index(drop=True)
                    sheet['properties'] = properties_df

        # add hyperlink to groupName column
        if args.group_id is not None:
            sheet['group_filtered'] = add_group_url(group_df, papi)
        if not allgroups_df.empty:
            sheet['account_summary'] = add_group_url(allgroups_df, papi)

    logger.debug(properties_df.columns.values.tolist()) if not properties_df.empty else None

    files.write_xlsx(filepath, sheet, freeze_column=6) if not properties_df.empty else None
    subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath]) if platform.system() == 'Darwin' and args.show is True and not properties_df.empty else None


def activate_from_excel(args):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    df = load_config_from_xlsx(papi, args.file, args.sheet, args.filter)

    network = args.network[0]
    note = args.note
    emails = args.email

    df['activationId'] = df[['propertyId', 'stagingVersion']].apply(lambda x: papi.activate_property_version(*x, network, note, emails), axis=1)
    df['activationId'] = df['activationId'].astype(int)

    logger.warning(f'Row count: {len(df)}')
    logger.warning(f'New activationId\n{df}')

    active = pd.DataFrame()
    while len(df) > len(active):
        df['production_status'] = df[['propertyId', 'activationId', 'stagingVersion']].swifter.apply(lambda x: papi.activation_status(*x), axis=1)
        active = df[df['production_status'] == 'ACTIVE'].copy()
        print()
        if len(df) == len(active):
            logger.critical(f'Activation Completed\n{active}')
        else:
            logger.info(f'Activation In Progress\n{df}')
            lg.countdown(60, msg='Checking again ... ')


def activation_status(args):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)

    # iterate over files in directory
    if args.directory:
        failed_configs = []
        for filename in os.listdir(args.directory):
            csv_file = os.path.join(args.directory, filename)
            if os.path.isfile(csv_file):
                logger.debug(csv_file)
                df = pd.read_csv(csv_file)
                df = df.fillna('')

                df = df[(df['production_activation_id'] > 0) & (df['staging_activation_id'] > 0)]
                # df = df[~df["comment"].str.contains('skip')]
                # df = df[~df["comment"].str.contains('SHP')]
                x = csv_file.rstrip('.csv')
                y = x.lstrip(f'{args.directory}/')

                df['groupId'] = y
                df['groupId'] = df['groupId'].astype(str)
                df['propertyId'] = df['propertyId'].astype(str)
                if not df.empty:
                    logger.debug(df)
                    df = df.sort_values(by=['groupId', 'propertyName'])
                    df = df.reset_index(drop=True)
                    failed_configs.append(df)

    # combine for all groups
    all_failed_configs = pd.concat(failed_configs, axis=0)
    all_failed_configs = all_failed_configs.reset_index(drop=True)
    # columns = ['groupId', 'propertyName', 'propertyId', 'basedVersion', 'new_version',
    #           'stg_error', 'prd_error', 'comment']
    # all_failed_configs = all_failed_configs[columns]
    # del df['staging_activation_id']
    # del df['production_activation_id']
    logger.warning(f'\n{all_failed_configs}')
    sheet = {}
    sheet['failled_list'] = all_failed_configs
    filepath = f'output/{args.output}'
    files.write_xlsx(filepath, sheet)

    if args.show:
        if platform.system() != 'Darwin':
            logger.info('--show argument is supported only on Mac OS')
        else:
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
    df = all_failed_configs
    df['staging_status'] = df[['propertyId', 'staging_activation_id', 'new_version']].swifter.apply(lambda x: papi.activation_status(*x), axis=1)
    df['production_status'] = df[['propertyId', 'production_activation_id', 'new_version']].swifter.apply(lambda x: papi.activation_status(*x), axis=1)

    columns = ['propertyName', 'propertyId', 'basedVersion', 'new_version', 'comment',
               'staging_activation_id', 'staging_status',
               'production_activation_id', 'production_status',
               'stg_error', 'prd_error']
    df = df.sort_values(by=['staging_activation_id', 'comment', 'propertyName'])
    df = df.reset_index(drop=True)
    logger.info(f'\n{df[columns]}')


def get_property_ruletree(args):
    '''
    python bin/akamai-utility.py -a 1-5BYUG1 delivery-config ruletree --property-id 219351 --version 27 --ruletree --show
    '''

    Path('output/ruletree').mkdir(parents=True, exist_ok=True)

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    for property_id in args.property_id:
        limit, full_ruletree = papi.get_property_limit(property_id, args.version)
        if args.show_limit:
            df = pd.DataFrame.from_dict(limit, orient='index')
            print(tabulate(df, headers=['value'], tablefmt='github', showindex='always'))

        ruletree = papi.get_property_ruletree(property_id, args.version)
        config, version = full_ruletree['propertyName'], full_ruletree['propertyVersion']
        title = f'{config}_v{version}'

        files.write_json(f'output/ruletree/{title}_limit.json', limit)
        files.write_json(f'output/ruletree/{title}_ruletree.json', ruletree)

        with open(f'output/ruletree/{title}_ruletree.json') as f:
            json_object = json.load(f)
        ruletree_json = json_object['rules']

        # write tree structure to TXT file
        # https://stackoverflow.com/questions/19330089/writing-string-representation-of-class-instance-to-file
        TREE_FILE = f'output/ruletree/{title}_ruletree_summary.txt'
        message = 'Rules tree depth'
        logger.info(f'{message:<20} {TREE_FILE}')
        with open(TREE_FILE, 'w') as file:
            print(files.tree_builder(ruletree_json, order=0, parent=0, level=0), file=file)
        files.remove_first_line_txt(TREE_FILE)

        # read file to show on terminal with line number
        with open(TREE_FILE) as file:
            rules = [line.strip() for line in file.readlines()]
            logger.debug('Tree Depth Detail')
            logger.debug('\n'.join(rules))

            if len(rules) == 1:
                logger.info(f'No nested rule found for property {config}')
                file_object = Path(TREE_FILE).absolute()
                file_object.unlink(missing_ok=False)

            # set alignment
            if len(rules) > 1:
                dot = '_'
                depth = [line.count('|') for line in rules]
                description_limit = 130
                max_depth = max(depth)
                logger.debug(f'{max_depth=}')
                if args.show_depth is False:
                    logger.warning('To display max depth, add --show-depth')
                tree_depth_dict = dict(zip(rules, depth))

                if max_depth > 0:
                    header = 'Rule #      Tree Depth.Rule #    Rule Name'
                    titles = [title, header]
                    with open(TREE_FILE, 'w') as file:
                        # add title to file
                        for each in titles:
                            print(f'{each}', file=file)
                        for i, line in enumerate(rules, 1):
                            total = description_limit - len(line)
                            print(f'{line}', file=file)

                # override TREE_FILE and include line number
                # Default rule is considered Zero
                # Print line number starts at 1
                if max_depth > 0 and args.show_depth:
                    with open(TREE_FILE, 'w') as file:
                        header_1 = f'Max nested tree: {max_depth}'
                        header_2 = 'Line No.    Rule #    Tree Depth.Rule #    Rule Name'
                        titles = [title, header_1, header_2]
                        for each in titles:
                            print(f'{each}', file=file)
                        for i, line in enumerate(rules):
                            total = description_limit - len(line)
                            if max_depth == line.count('|'):
                                print(f'{i+1:<15}{line}{dot*total}{dot*5}{max_depth}', file=file)
                            else:
                                print(f'{i+1:<15}{line}', file=file)

            if platform.system() != 'Darwin':
                pass
            else:
                command = ['code', Path(TREE_FILE).absolute()]
                try:
                    Popen(command, stdout=os.open(os.devnull, os.O_RDWR), stderr=STDOUT)
                except:
                    subprocess.call(['open', '-a', 'TextEdit', Path(TREE_FILE).absolute()])


def hostnames(args):
    '''
    python bin/akamai-utility.py -a 1-5BYUG1 delivery-config hostname --property --version 27 --show
    '''

    if args.version and len(args.property) > 1:
        sys.exit(logger.error('If --version is specified, we can lookup one property'))

    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = account.replace(' ', '_')
    logger.warning(f'Found account {account}')
    account = re.sub(r'[.,]|(_Direct_Customer|_Indirect_Customer)|_', '', account)
    filepath = f'output/{account}_hostname_certificate.xlsx' if args.output is None else f'output/{args.output}'

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)

    sheet = {}
    print()
    for property in args.property:
        logger.warning(f'Lookup property {property}')
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.info(f'property {property:<50} not found')
            break
        else:
            stg, prd = papi.property_version(resp)
            version = prd

            hostname = papi.get_property_version_hostnames(papi.property_id, version)
            df = pd.json_normalize(hostname)
            logger.debug(df.columns.values.tolist())

            columns = ['Property_Hostname', 'Edge_Hostname', 'certProvisioningType', 'production', 'staging']
            df = df.rename(columns={'cnameFrom': 'Property_Hostname',
                                    'cnameTo': 'Edge_Hostname',
                                    'certStatus.production': 'production',
                                    'certStatus.staging': 'staging'})

            if 'production' in df.columns.values.tolist():
                df['production'] = df['production'].apply(lambda x: x[0].get('status') if isinstance(x, list) else x)
                df['staging'] = df['staging'].apply(lambda x: x[0].get('status') if isinstance(x, list) else x)
                df = df.sort_values(by=['production', 'Property_Hostname'])
                df = df.reset_index(drop=True)

            sheetname = f'{property}_v{version}'
            if len(sheetname) >= 31:
                sheet[f'{papi.property_id}_v{version}'] = df
            else:
                sheet[sheetname] = df

    files.write_xlsx(filepath, sheet, freeze_column=3, adjust_column_width=True)
    if platform.system() == 'Darwin' and args.no_show is False:
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
    else:
        logger.info('--no-show argument is supported only on Mac OS')


def get_property_all_behaviors(args):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config behavior --property XXXXX --remove-tags uuid variables templateUuid templateLink xml
    '''
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    status, resp = papi.search_property_by_name(args.property)
    if status == 200:
        version = int(args.version) if args.version else None
        if version is None:
            stg, prd = papi.property_version(resp)
            version = prd

    tree_status, json_response = papi.property_ruletree(papi.property_id, version, args.remove_tags)
    if tree_status == 200:
        # print_json(data=json_response)
        behaviors = papi.get_property_behavior(json_response['rules'])
        unique_behaviors = sorted(list(set(behaviors)))
        logger.debug(unique_behaviors)
        behaviors_cli = '[' + ' '.join(unique_behaviors) + ']'

        if len(unique_behaviors) > 0:
            logger.info('Behaviors founded')
            logger.warning(behaviors_cli)
            print()
            logger.critical('You can use the list to compare behavior between 2 delivery configs')
            logger.info('>> akamai util diff behavior --property A B --behavior allHttpInCacheHierarchy allowDelete allowOptions allowPatch allowPost')


def get_property_advanced_behavior(args):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config metadata --property xxx yyy --advBehavior
    '''

    if args.version and len(args.property) > 1:
        sys.exit(logger.error('If --version is specified, we can lookup one property'))

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    property_dict = {}
    property_list = []
    sheet = {}

    logger.warning('Searching for advanced behavior ...')
    for property in args.property:
        logger.warning(f'Lookup property {property}')
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.info(f'property {property:<50} not found')
            break
        else:
            stg, prd = papi.property_version(resp)
            version = prd
            if args.version:
                version = args.version
                logger.critical(f'lookup requested v{version}')

        excel_sheet, xml_data = papi.get_property_advanced_behavior_xml(papi.property_id,
                                                                        version,
                                                                        displayxml=args.hidexml,
                                                                        showlineno=args.lineno)

        if not xml_data:
            logger.critical(f'{excel_sheet:<50} no advanced behavior')
        else:
            logger.debug(excel_sheet)
            print()
            property_dict[papi.property_name] = [xml_data]
            property_list.append(property_dict)
            sheet_df = pd.DataFrame.from_dict(xml_data, orient='index', columns=['advancedBehavior'])
            sheet_df.index.name = excel_sheet
            sheet_df = sheet_df.reset_index()

            if not sheet_df.empty:
                highlighted_tags = []
                # add highlighted tags as a new column in the DataFrame
                for xml_string in sheet_df['advancedBehavior']:
                    syntax = Syntax(xml_string, 'xml', theme='solarized-dark', line_numbers=True)
                    highlighted_tags.append(str(syntax.highlight(code=xml_string)))

                sheet_df['advancedBehavior'] = highlighted_tags

                if len(excel_sheet) > 26:
                    excel_sheet = f'{papi.property_id}_v{version}'

                sheet[excel_sheet] = sheet_df

                # print the table with syntax highlighting
                '''
                table = tabulate(sheet_df, headers='keys', tablefmt='simple')
                if args.hidexml is True:
                    console = Console()
                    console.print(table)
                '''

    if sheet:
        print()
        filepath = 'advancedBehavior.xlsx'
        files.write_xlsx(filepath, sheet, show_index=True)
        if args.no_show is False and platform.system() == 'Darwin':
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
        else:
            logger.info('--show argument is supported only on Mac OS')


def get_property_advanced_match(args):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config metadata --property xxx yyy --advMatch
    '''
    if args.version and len(args.property) > 1:
        sys.exit(logger.error('If --version is specified, we can lookup one property'))

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    property_dict = {}
    property_list = []
    sheet = {}
    print()

    logger.warning('Searhing for advanced match ...')
    for property in args.property:
        logger.warning(f'Lookup property {property}')
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.info(f'property {property:<50} not found')
            break
        else:
            stg, prd = papi.property_version(resp)
            version = prd
            if args.version:
                version = args.version
                logger.critical(f'lookup requested v{version}')

        excel_sheet, xml_data = papi.get_property_advanced_match_xml(papi.property_id,
                                                                     version,
                                                                     displayxml=args.hidexml,
                                                                     showlineno=args.lineno)
        if not xml_data:
            logger.critical(f'{excel_sheet:<50} no advanced match')
        else:
            property_dict[papi.property_name] = [xml_data]
            property_list.append(property_dict)
            sheet_df = pd.DataFrame.from_dict(xml_data, orient='index', columns=['advancedMatch'])
            sheet_df.index.name = excel_sheet
            sheet_df = sheet_df.reset_index()

            if sheet_df.empty:
                logger.warning(f'no advancedMatch found for property {property}_v{version}')
            else:
                # print the table with syntax highlighting
                table = tabulate(sheet_df, headers='keys', tablefmt='simple')
                if args.hidexml is True:
                    console = Console()
                    console.print(table)

                if len(excel_sheet) > 26:
                    excel_sheet = f'{papi.property_id}_v{version}'

                sheet[excel_sheet] = sheet_df

    if sheet:
        print()
        filepath = 'advancedMatch.xlsx'
        files.write_xlsx(filepath, sheet, show_index=True)
        if args.no_show is False and platform.system() == 'Darwin':
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
        else:
            logger.info('--show argument is supported only on Mac OS')


def get_property_advanced_override(args):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config metadata --property xxx yyy --advOverride
    '''
    if args.version and len(args.property) > 1:
        sys.exit(logger.error('If --version is specified, we can lookup one property'))

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    console = Console()
    property_dict = {}
    property_list = []
    sheet = {}
    print()
    logger.warning('Searching for advanced override ...')
    for property in args.property:
        logger.warning(f'Lookup property {property}')
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.info(f'property {property:<50} not found')
            break
        else:
            stg, prd = papi.property_version(resp)
            version = prd
            if args.version:
                version = args.version
                logger.critical(f'lookup requested v{version}')

        _ = papi.get_property_ruletree(papi.property_id, version)

        title = f'{property}_v{version}'
        adv_override = papi.get_property_advanced_override(papi.property_id, version)
        if adv_override:
            property_dict[title] = [adv_override]
            property_list.append(property_dict)
            sheet_df = pd.DataFrame.from_dict({'advancedOverride': adv_override}, orient='index', columns=['adv_override'])
            sheet_df.index.name = title
            sheet_df = sheet_df.reset_index()

            sheet_name = f'{property}_v{version}'
            if len(sheet_name) > 26:
                sheet_name = f'{papi.property_id}_v{version}'

            sheet[sheet_name] = sheet_df

            if args.hidexml is True:
                syntax = Syntax(adv_override, 'xml', theme='solarized-dark', line_numbers=args.lineno)
                console.print(syntax)
        else:
            logger.critical(f'{title:<50} no advanced override')

    if sheet:
        print()
        filepath = 'advancedOverride.xlsx'
        files.write_xlsx(filepath, sheet, show_index=True)
        if args.no_show is False and platform.system() == 'Darwin':
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
        else:
            logger.info('--show argument is supported only on Mac OS')


# BEGIN helper method
def load_config_from_xlsx(papi, filepath: str, sheet_name: str | None = None, filter: str | None = None):
    '''
    excel must have header rows
    '''
    df = pd.read_excel(f'{filepath}', sheet_name=sheet_name, index_col=None)
    if filter:
        mask = np.column_stack([df[col].astype(str).str.contains(fr'{filter}', na=False) for col in df])
        df = df.loc[mask.any(axis=1)]
    df['stagingVersion'] = df['stagingVersion'].astype(int)
    df['productionVersion'] = df['productionVersion'].astype(int)
    if 'activationId' in df.columns.values.tolist():
        df['activationId'] = df['activationId'].astype(int)

    df['url'] = df.apply(lambda row: papi.property_url(row['assetId'], row['groupId']), axis=1)

    columns = ['propertyId', 'propertyName', 'stagingVersion', 'productionVersion']
    if 'activationId' in df.columns.values.tolist():
        columns.append('activationId')
    df = df[columns].copy()
    logger.info(f'Original Data from Excel\n{df}')
    return df[columns]


def add_group_url(df: pd.DataFrame, papi) -> pd.DataFrame:
    df['groupURL'] = df.apply(lambda row: papi.group_url(row['groupId']), axis=1)
    df['groupName_url'] = df.apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['groupURL'], row['groupName']), axis=1)
    del df['groupURL']
    del df['groupName']
    df = df.rename(columns={'groupName_url': 'groupName'})  # show column with hyperlink instead
    if 'parentGroupId' in df.columns.values.tolist():
        summary_columns = ['group_structure', 'groupName', 'groupId', 'parentGroupId', 'contractId', 'propertyCount']
    else:
        summary_columns = ['group_structure', 'groupName', 'groupId', 'contractId', 'propertyCount']
    return df[summary_columns]
# END helper method
