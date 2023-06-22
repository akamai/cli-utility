from __future__ import annotations

import copy
import os
import platform
import subprocess
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

import pandas as pd
from akamai_api.appsec import Appsec
from akamai_api.papi import Papi
from akamai_utils import papi as p
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import diff_html as compare
from utils import files
from utils.parser import AkamaiParser as Parser


logger = lg.setup_logger()


def collect_json(config_name: str, version: int, response_json):
    Path('output/diff/json').mkdir(parents=True, exist_ok=True)
    config_name = config_name.replace(' ', '_')
    dt_string = datetime.now().strftime('%Y%m%d_%H%M%s')
    json_file = f'output/diff/json/{config_name}_v{version}_{dt_string}.json'
    logger.debug(json_file)
    files.write_json(json_file, response_json)
    return json_file


# get delivery config json
def delivery_config_json(papi, config: str, version: int | None = None, exclude: list | None = None):
    status, _ = papi.search_property_by_name(config)
    if status == 200:
        json_tree_status, json_response = papi.property_ruletree(papi.property_id, version, exclude)
    if json_tree_status == 200:
        return collect_json(config, version, json_response)
    else:
        logger.debug(f'{papi.property_id=} {config=} {status=} {version} {json_tree_status}')
        sys.exit(logger.error(f"{config=} {json_response['title']}: {version}"))


# get security config json
def security_config_json(appsec, waf_config_name: str, config_id: int, version: int | None = None, exclude: list | None = None):
    status, sec_response = appsec.get_config_version_detail(config_id, version)
    logger.debug(f'{waf_config_name} {config_id=} {status=} {version}')
    if status == 200:
        _, sec_response = appsec.get_config_version_detail(config_id, version, exclude)
        return collect_json(f'{config_id}_{waf_config_name}', version, sec_response)
    else:
        print_json(data=sec_response)
        sys.exit(logger.error(sec_response['detail']))


def compare_versions(v1: str, v2: str, outputfile: str, args):
    print('\n\n')
    cmd_text = f'diff -u {v1} {v2} | ydiff -s --wrap -p cat'
    subprocess.run(cmd_text, shell=True)

    output_path = f'output/diff/{outputfile}.html'
    compare.main(v1, v2, output_path, args)
    location = f'{os.path.abspath(output_path)}'

    print()
    space = ' ' * 33
    logger.info(f'left file:{space}{v1}')
    space = ' ' * 32
    logger.info(f'right file:{space}{v2}')
    return location


def config(args):

    config1, config2, left, right, cookies = args.config1, args.config2, args.left, args.right, args.acc_cookies
    logger.debug(f'{config1=} {config2=} {cookies=}')

    papi = Papi(account_switch_key=args.account_switch_key, section=args.section, cookies=args.acc_cookies)
    appsec = Appsec(account_switch_key=args.account_switch_key, section=args.section, cookies=args.acc_cookies)

    if args.xml is True:
        Path('output/diff/xml').mkdir(parents=True, exist_ok=True)
        papi.account_id = papi.get_account_id()

        # First check X-Xsrf-Token
        msg = 'Cookie information from control.akamai.com are required to compare XML metadata'
        msg = f'{msg}\n\t Cookies name required: XSRF-TOKEN, AKASSO, and AKATOKEN'
        msg = f'{msg}\n\n\t You can set them up inside .edgerc file or add additional arguments --acc-cookies'
        if not papi.cookies and args.acc_cookies is None:
            sys.exit(logger.error(msg))

        if not appsec.cookies and args.acc_cookies is None and args.security:
            sys.exit(logger.error(msg))

    # compare delivery config
    if args.security is False:
        status, response = papi.search_property_by_name(config1)

        if config2 is None:
            if left is None and right:
                sys.exit(logger.error('Missing --left argument and integer value of version'))
            elif right is None and left:
                sys.exit(logger.error('Missing --right argument and integer value of version'))
            elif all([left, right]):
                if left == right:
                    sys.exit(logger.error('Same version, nothing to compare'))
            else:
                if status == 200:
                    stg_version, prd_version = papi.property_version(response)
                    left = stg_version
                    right = prd_version
                else:
                    sys.exit(logger.error(print_json(data=response)))

            if left == right:
                sys.exit(logger.error('Same version, nothing to compare'))
            else:
                print()
                v1 = delivery_config_json(papi, config1, left, args.remove_tags)
                v2 = delivery_config_json(papi, config1, right, args.remove_tags)

        if config2:
            if not all([left, right]):
                sys.exit(logger.error('Missing --left and --right argument and integer value of version'))
            # this value will override config1.v2 when --config2 is provided
            print()
            v1 = delivery_config_json(papi, config1, left, args.remove_tags)
            v2 = delivery_config_json(papi, config2, right, args.remove_tags)

    # compare security config
    if args.security is True:
        # input is config_id, not config name
        status_code, response = appsec.get_config_detail(config1)
        if status_code == 200:
            waf_config_name = response['name']
            waf_config_name = waf_config_name.replace(' ', '_')
            print()
            logger.warning(f"Found security config name '{waf_config_name}' config_id {config1}")
        elif status_code in [400, 403]:
            # list all configs to user on terminal
            status_code, resp_1 = appsec.list_waf_configs()
            if status_code == 200:
                df = pd.DataFrame(resp_1)
                df = df[df['name'] == config1].copy()
                if not df.empty:
                    status_code, resp_2 = appsec.get_config_detail(df['id'].values[0])
                    if status_code == 200:
                        waf_config_name = resp_2['name']
                        waf_config_name = waf_config_name.replace(' ', '_')
                        config1 = resp_2['id']
                        response = resp_2
                        logger.warning(f"Found security config name '{waf_config_name}' config_id {config1}")
                else:
                    if args.name_contains:
                        df = pd.DataFrame(resp_1)
                        df = df[df['name'].str.contains(args.name_contains, case=False)].copy()

                        if not df.empty:
                            if df.shape[0] == 1:
                                config1 = df['id'].values[0]
                                waf_config_name = df['name'].values[0]
                                waf_config_name = waf_config_name.replace(' ', '_')
                                config1 = df['id'].values[0]
                                response = resp_1
                            else:
                                print(tabulate(df[['name', 'id']], headers='keys', tablefmt='psql', showindex=False))
                                sys.exit(logger.error(f'Security config id "{config1}" not found, please review a list and use id from table shown'))

                if df.empty:
                    df = pd.DataFrame(resp_1)
                    response = resp_1
                    print(tabulate(df[['name', 'id']], headers='keys', tablefmt='psql', showindex=False))
                    sys.exit(logger.error(f'Security config id "{config1}" not found, please review a list and use id from table shown'))

        if config2 is None:
            if left is None and right:
                sys.exit(logger.error('Missing --left argument and integer value of version'))
            elif right is None and left:
                sys.exit(logger.error('Missing --right argument and integer value of version'))
            elif all([left, right]):
                if left == right:
                    sys.exit(logger.error(f'Same version {left}, nothing to compare'))
            else:
                try:
                    stg_version = response['stagingVersion']
                except:
                    stg_version = 0
                try:
                    prd_version = response['productionVersion']
                except:
                    prd_version = 0

                left = stg_version
                right = prd_version
                if left == right:
                    sys.exit(logger.error(f'Same version {left}, nothing to compare'))

            v1 = security_config_json(appsec, waf_config_name, config1, left, args.remove_tags)
            v2 = security_config_json(appsec, waf_config_name, config1, right, args.remove_tags)

        if config2:
            if not all([left, right]):
                sys.exit(logger.error('Missing --left and --right argument and integer value of version'))
            v1 = security_config_json(appsec, waf_config_name, config1, left, args.remove_tags)
            v2 = security_config_json(appsec, waf_config_name, config2, right, args.remove_tags)

    title = 'Compare report in HTML format is saved at: '
    if args.json is True:
        print()
        logger.warning('Comparing JSON configuration')
        json_index_html = compare_versions(v1, v2, 'index_json', args)
        if not args.no_show:
            webbrowser.open(f'file://{os.path.abspath(json_index_html)}')
        else:
            logger.info(f'{title}{os.path.abspath(json_index_html)}')

    if args.xml is True:
        print()
        logger.warning('Comparing XML Metadata')
        if args.security is False:
            logger.debug(f'{papi.property_id} {papi.asset_id} {papi.group_id}')
            v1_xml = papi.get_properties_version_metadata_xml(config1, papi.asset_id, papi.group_id, left)
            v2_xml = papi.get_properties_version_metadata_xml(config1, papi.asset_id, papi.group_id, right)

            xml_index_html = compare_versions(v1_xml, v2_xml, 'index_delivery', args)
            if not args.no_show:
                webbrowser.open(f'file://{os.path.abspath(xml_index_html)}')
            else:
                logger.info(f'{title}{os.path.abspath(xml_index_html)}')

        if args.security is True:
            v1_xml = appsec.get_config_version_metadata_xml(waf_config_name, left)
            v2_xml = appsec.get_config_version_metadata_xml(waf_config_name, right)

            v1 = v1_xml['portalWaf']
            v2 = v2_xml['portalWaf']
            xml_portalWaf_index_html = compare_versions(v1, v2, 'index_xml_portalWaf', args)
            if not args.no_show:
                webbrowser.open(f'file://{os.path.abspath(xml_portalWaf_index_html)}')
            else:
                logger.info(f'{title}{os.path.abspath(xml_portalWaf_index_html)}')

            v1 = v1_xml['wafAfter']
            v2 = v2_xml['wafAfter']
            xml_wafAfter_index_html = compare_versions(v1, v2, 'index_xml_wafAfter', args)
            if not args.no_show:
                webbrowser.open(f'file://{os.path.abspath(xml_wafAfter_index_html)}')
            else:
                logger.info(f'{title}{os.path.abspath(xml_wafAfter_index_html)}')


def config_behaviors(args):
    '''
    python bin/akamai-utility.py -a 1-1S6D diff behavior --property api.nike.com_pm ecn-api.nike.com_pm --left 1372 --right 1143 \
        --remove-tags advanced uuid variables templateUuid templateLink xml \
        --behavior allHttpInCacheHierarchy allowDelete allowOptions allowPatch allowPost
    '''
    properties, left, right = args.property, args.left, args.right
    papi = Papi(account_switch_key=args.account_switch_key, section=args.section)
    prop = {}
    all_properties = []
    for i, property in enumerate(properties):
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.info(f'{status} {resp}')
        else:
            if not (left and right):
                stg, prd = papi.property_version(resp)
                logger.warning(f'{property:<50} {papi.property_id}')
                property_name = f'{property}_{prd}'
                all_properties.append(property_name)
                status, json = papi.property_ruletree(papi.property_id, prd, args.remove_tags)
            else:
                if left and i == 0:
                    property_name = f'{property}_v{left}'
                    logger.warning(f'{property:<50} {papi.property_id}')
                    all_properties.append(property_name)
                    status, json = papi.property_ruletree(papi.property_id, left, args.remove_tags)
                if right and i == 1:
                    property_name = f'{property}_v{right}'
                    logger.warning(f'{property:<50} {papi.property_id}')
                    all_properties.append(property_name)
                    status, json = papi.property_ruletree(papi.property_id, right, args.remove_tags)

            if status == 200:
                prop[property_name] = json['rules']

    papi_rules = p.PapiWrapper(account_switch_key=args.account_switch_key)
    behaviors = {}
    print()
    summary = []
    for behavior in args.behavior:
        behavior_dict = {}
        for property_name, rule in prop.items():
            prop_behavior = {}
            # need current_path and paths, otherwise getting duplicates
            prop_behavior = papi_rules.get_property_path_n_rule(rule, behavior, current_path='', paths=[])
            logger.debug(f'{property_name:<50} {behavior:<50} {len(prop_behavior):<50}')
            summary.append((property_name, behavior, len(prop_behavior)))
            # print_json(data=prop_behavior)
            behavior_dict[property_name] = copy.deepcopy(prop_behavior)
        behaviors[behavior] = behavior_dict

    sheet = {}
    temp_df = pd.DataFrame(summary, columns=['property', 'behavior', 'count'])
    summary_df = temp_df.pivot(index='behavior', columns='property', values='count')
    columns = summary_df.columns.values.tolist()

    summary_df['note_1'] = summary_df[columns[1]].apply(lambda x: f'not in {columns[1]}' if x == 0 else '')
    summary_df = summary_df.sort_values(by=['note_1', 'behavior'], ascending=[False, True])
    sheet['summary'] = summary_df
    logger.debug(f'\n{summary_df}')
    print(tabulate(summary_df, headers='keys', tablefmt='simple'))

    print()
    for behavior, prop_data in behaviors.items():
        logger.debug(f'{behavior} {len(prop_data)} {type(prop_data)}')
        df = pd.DataFrame(prop_data.items(), columns=['property', 'behavior'])
        df = df.explode('behavior').reset_index(drop=True)
        df['path'] = df['behavior'].apply(lambda x: list(x.keys())[0] if isinstance(x, dict) else None)
        df['rules'] = df['behavior'].apply(lambda x: list(x.values())[0] if isinstance(x, dict) else '')
        df['rules'] = df['rules'].apply(str)
        df = df.fillna('')
        df = df.sort_values(by=['rules', 'property'])
        columns = ['property', 'path', 'rules']
        df = df[columns]
        logger.debug(f'\n{df}')
        sheet[behavior] = df

        logger.debug(behavior)
        stat = df.groupby(['rules'], as_index=False)['property'].count()
        logger.debug(f'\n{stat}')
        stat['even_number'] = stat['property'].apply(lambda x: x % 2 == 0)

        if len(behavior) > 26:
            behavior = behavior[1:20]
        sheet[f'{behavior}_stat'] = stat

    filepath = f'output/{all_properties[0]}.xlsx'
    files.write_xlsx(filepath, sheet, show_index=True, adjust_column_width=False, freeze_column=3)

    if args.no_show is False and platform.system() == 'Darwin':
        subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
    else:
        logger.info('--show argument is supported only on Mac OS')
