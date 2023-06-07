from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from subprocess import Popen
from subprocess import STDOUT

import numpy as np
import pandas as pd
import swifter
from akamai_api.identity_access import IdentityAccessManagement
from akamai_utils import papi as p
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


def activate_from_excel(args):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    if args.load:
        df = load_config_from_xlsx(args.load, args.sheet, args.filter, papi)

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

    else:
        network = args.network[0]
        for property_id in args.property_id:
            status_code, resp = papi.activation_property_version(args.property_id, args.version, network, list(args.note), list(args.email))
            try:
                activation_id = resp.json()['activationLink'].split('?')[0].split('/')[-1]
            except:
                activation_id = 0
            logger.info(f'{property_id},{args.version},{network},{status_code},{activation_id},{resp}')


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


def main(args):
    '''
    python bin/akamai-utility.py -a 1-1IY5Z delivery-config --show --group-id 14803 163889 162428 90428 14805 82695
    2173 properties 90 minutes
     800 properties 30 minutes
    '''

    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key)
    account = iam.search_account_name(value=args.account_switch_key)
    try:
        account = f"{account[0]['accountName']}".replace(' ', '_')
        logger.warning(f'Found account: {account}')
        account = account.replace('.', '_')
        filepath = f'output/{account}.xlsx' if args.output is None else f'output/{args.output}'
    except:
        print_json(data=account)
        lg.countdown(540, msg='Oopsie! You just hit rate limit.')
        sys.exit(logger.error(account['detail']))

    # build group structure as displayed on control.akamai.com
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    allgroups_df, columns = papi.account_group_summary()

    sheet = {}
    allgroups_df['groupId'] = allgroups_df['groupId'].astype(str)  # change groupId to str before load into excel
    sheet['summary'] = allgroups_df

    if args.group_id:
        groups = args.group_id
        group_df = allgroups_df[allgroups_df['groupId'].isin(groups)].copy()
        group_df = group_df.reset_index(drop=True)
        sheet['filter'] = group_df
    else:
        group_df = allgroups_df[allgroups_df['propertyCount'] > 0].copy()
        group_df = group_df.reset_index(drop=True)
    print()
    print(tabulate(group_df, headers=columns, showindex=True, tablefmt='github'))

    # warning for large account
    if not args.group_id:
        print()
        logger.warning(f'total groups {allgroups_df.shape[0]}, only {group_df.shape[0]} groups have properties.')
        total = allgroups_df['propertyCount'].sum()
        if total > 100:
            print()
            logger.critical(f'This account has {total} properties, please be patient')
            logger.critical('200 properties take ~ 7 minutes')
            logger.critical('800 properties take ~ 30 minutes')
            logger.critical('please consider using --group-id to reduce total properties')

    # collect properties detail for all groups
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
                logger.debug(properties_df.dtypes)
                columns = ['accountId', 'contractId', 'groupName', 'groupId',
                        'assetId', 'propertyId', 'propertyName', 'propertyURL',
                        'latestVersion', 'stagingVersion', 'productionVersion',
                        'updatedDate', 'ruleFormat',
                        'hostname_count', 'hostname']
                properties_df = properties_df[columns]
                sheet['properties'] = properties_df
    files.write_xlsx(filepath, sheet, freeze_column=6)

    if args.show:
        if platform.system() != 'Darwin':
            logger.info('--show argument is supported only on Mac OS')
        else:
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])


def load_config_from_xlsx(filepath: str, sheet_name: str, filter: str, papi):
    '''
    excel must have headers assetId and groupId
    '''
    df = pd.read_excel(f'{filepath}', sheet_name=sheet_name, index_col=None)
    if filter:
        mask = np.column_stack([df[col].astype(str).str.contains(fr'{filter}', na=False) for col in df])
        df = df.loc[mask.any(axis=1)]
    df['stagingVersion'] = df['stagingVersion'].astype(int)
    df['productionVersion'] = df['productionVersion'].astype(int)
    if 'activationId' in df.columns.values.tolist():
        df['activationId'] = df['activationId'].astype(int)

    # df['url'] = df.apply(lambda row: papi.property_url(row['assetId'], row['groupId']), axis=1)

    columns = ['propertyName', 'propertyId', 'Batch', 'stagingVersion', 'productionVersion']
    if 'activationId' in df.columns.values.tolist():
        columns.append('activationId')
    df = df[columns].copy()
    logger.info(f'Original Data from Excel\n{df}')
    return df[columns]


def get_property_ruletree(args):
    '''
    python bin/akamai-utility.py -a 1-5BYUG1 delivery-config --property-id 219351 --version 27 --ruletree --show
    '''

    Path('output/ruletree').mkdir(parents=True, exist_ok=True)

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    for property_id in args.property_id:
        limit = papi.get_property_limit(property_id, args.version)

        df = pd.DataFrame.from_dict(limit, orient='index')
        print(tabulate(df, headers='keys', tablefmt='github', numalign='center', showindex='always'))

        ruletree = papi.get_property_ruletree(property_id, args.version)
        config, version = ruletree['propertyName'], ruletree['propertyVersion']
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
                logger.warning('To display max depth, add --show y')
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
                if max_depth > 0 and args.show:
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


def get_property_advanced_metadata(args):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config --advancedmetadata --property-id 743088 672055 --version 10
    '''
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    property_dict = {}
    console = Console()
    property_list = []
    sheet = {}
    for property_id in args.property_id:
        ruletree = papi.get_property_ruletree(property_id, args.version)
        config, version = ruletree['propertyName'], ruletree['propertyVersion']
        title = f'{config}_v{version}'

        files.write_json(f'output/ruletree/{title}_ruletree.json', ruletree)

        with open(f'output/ruletree/{title}_ruletree.json') as f:
            json_object = json.load(f)

        property_name = f"{property_id}_{json_object['propertyName']}_v{json_object['propertyVersion']}"
        excel_sheet = f"{property_id}_v{json_object['propertyVersion']}"
        logger.warning(property_name)

        target_data = []
        ruletree_json = json_object['rules']
        papi.find_name_and_xml(ruletree_json, target_data)

        xml_data = {}
        for index, item in enumerate(target_data):
            xml_data[item['name']] = item['xml']
            logger.debug(f"{index:>3}: {item['name']}")

            property_dict[property_name] = [xml_data]
            '''
            if args.filter:
                if item['name'] == args.filter:
                    syntax = Syntax(item['xml'], "xml", theme="solarized-dark", line_numbers=True)
                    console.print(syntax)
            '''
        property_list.append(property_dict)

        sheet_df = pd.DataFrame.from_dict(xml_data, orient='index', columns=[f'xml_{excel_sheet}'])
        sheet_df.index.name = excel_sheet
        sheet[excel_sheet] = sheet_df

    logger.info(property_dict.keys())
    first = list(property_dict.keys())[0]
    if len(property_list) > 1:
        second = list(property_dict.keys())[1]

        logger.critical('Same rule name but have different XML')
        rules = same_rule(property_dict, first, second)
        for rule in rules:
            if not compare_xml(property_dict, first, second, rule):
                print()
                logger.warning(f' {rule}')
                v1 = 'xml_first.xml'
                v2 = 'xml_second.xml'
                xml1 = property_dict[first][0][rule]
                with open(v1, 'w') as f:
                    f.write(xml1)
                # syntax = Syntax(xml1, "xml", theme="solarized-dark", line_numbers=True)
                # console.print(syntax)

                xml2 = property_dict[second][0][rule]
                with open(v2, 'w') as f:
                    f.write(xml2)
                # syntax = Syntax(xml2, "xml", theme="solarized-dark", line_numbers=True)
                # console.print(syntax)
                cmd_text = f'diff -u {v1} {v2} | ydiff -s --wrap -p cat'
                subprocess.run(cmd_text, shell=True)

        print()
        logger.critical('Checking XML with different rule name')
        rules = different_rule(property_dict, first, second)
        for rule in rules:
            compare_xml(property_dict, first, second, rule)

    df = pd.DataFrame(property_list)
    sheet['complete'] = df
    filepath = 'test.xlsx'
    print()
    files.write_xlsx(filepath, sheet, freeze_column=1, show_index=True)

    if args.show:
        if platform.system() != 'Darwin':
            logger.info('--show argument is supported only on Mac OS')
        else:
            subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])


def same_rule(properties: dict, first: str, second: str) -> list:
    left = list(properties[first][0].keys())
    right = list(properties[second][0].keys())
    same_rule = list(set(left) & set(right))
    return same_rule


def different_rule(properties: dict, first: str, second: str) -> list:
    left = list(properties[first][0].keys())
    right = list(properties[second][0].keys())
    different_rule = list(set(left) - set(right))
    different_rule.extend(list(set(right) - set(left)))
    return different_rule


def compare_xml(properties: dict, first: str, second: str, rule: str) -> bool:
    try:
        xml_1 = properties[first][0][rule]
    except KeyError:
        xml_1 = 0
        logger.info(f' {rule:<30} not found in {first}')
    try:
        xml_2 = properties[second][0][rule]
    except KeyError:
        xml_2 = 0
        logger.info(f' {rule:<30} not found in {second}')
    return xml_1 == xml_2
