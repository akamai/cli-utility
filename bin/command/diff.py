from __future__ import annotations

import copy
import os
import platform
import subprocess
import sys
import webbrowser
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
from akamai_api.papi import Papi
from akamai_utils import appsec as a
from akamai_utils import papi as p
from command.parser import AkamaiParser as Parser
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import dataframe
from utils import diff_html as compare
from utils import files


def collect_json(config_name: str, version: int, response_json, logger=None):
    output_dir = 'output/0_diff/json'
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    config_name = config_name.replace(' ', '_')
    # dt_string = datetime.now().strftime('%Y%m%d_%H%M%s')
    # json_file = f'output/0_diff/json/{config_name}_v{version}_{dt_string}.json'
    json_file = f'output/0_diff/json/{config_name}_v{version}.json'
    logger.debug(json_file)
    files.write_json(json_file, response_json)
    return json_file


def delivery_config_json(papi, config: str, version: int | None = None, exclude: list | None = None, logger=None):
    status, response = papi.search_property_by_name(config)
    if status == 200:
        logger.debug(f'{papi.property_id=} {version=}')
        json_tree_status, json_response = papi.property_ruletree(papi.property_id, version, exclude)

        _ = papi.get_property_version_detail(papi.property_id, version)
        if json_tree_status == 200:
            return collect_json(config, version, json_response, logger=logger)
        else:
            logger.debug(f'{papi.property_id=} {config=} {status=} {version} {json_tree_status}')
            sys.exit(logger.error(f"{config=} {json_response['title']}: {version}"))
    else:
        sys.exit(logger.error(response))


def security_config_json(appsec, waf_config_name: str, config_id: int,
                         version: int | None = None,
                         exclude: list | None = None,
                         logger=None):
    status, sec_response = appsec.get_config_version_detail(config_id, version)
    logger.debug(f'{waf_config_name} {config_id=} {status=} {version}')
    if status == 200:
        _, sec_response = appsec.get_config_version_detail(config_id, version, exclude)
        return collect_json(f'{config_id}_{waf_config_name}', version, sec_response, logger=logger)
    else:
        print_json(data=sec_response)
        sys.exit(logger.error(sec_response['detail']))


def compare_versions(v1: str, v2: str, outputfile: str, args, logger=None):
    if platform.system() != 'Darwin':
        sys.exit(logger.info('diff command only support on non-Window OS'))

    cmd_text = f'diff -u {v1} {v2} | ydiff -s --wrap -p cat'

    diff = subprocess.run(cmd_text, shell=True, capture_output=True)

    if diff.stdout == b'':
        logger.info('no difference')
    else:
        diff = subprocess.run(cmd_text, shell=True)
        print()
        output_path = f'output/0_diff/{outputfile}.html'
        compare.main(v1, v2, output_path, args)
        location = f'{os.path.abspath(output_path)}'
        space = ' ' * 33
        logger.info(f'left file:{space}{v1}')
        space = ' ' * 32
        logger.info(f'right file:{space}{v2}')
        return location


def compare_config(args, logger=None):

    config1, config2, left, right, cookies = args.config1, args.config2, args.left, args.right, args.acc_cookies
    logger.debug(f'{config1=} {config2=} {cookies=}')

    papi = Papi(account_switch_key=args.account_switch_key, section=args.section, cookies=args.acc_cookies, logger=logger)
    appsec = a.AppsecWrapper(account_switch_key=args.account_switch_key, section=args.section, cookies=args.acc_cookies, logger=logger)

    if args.xml is True:
        Path('output/0_diff/xml').mkdir(parents=True, exist_ok=True)
        papi.account_id = papi.get_account_id()

        # First check X-Xsrf-Token
        msg = 'Cookie information from control.akamai.com are required to compare XML metadata'
        msg = f'{msg}\n\t  Cookies name required: XSRF-TOKEN, AKASSO, and AKATOKEN'
        msg = f'{msg}\n\n\t  You can set them up inside .edgerc file or add additional arguments --acc-cookies'
        if not papi.cookies and args.acc_cookies is None:
            sys.exit(logger.error(msg))

        if not appsec.cookies and args.acc_cookies is None and args.security:
            sys.exit(logger.error(msg))

    # compare delivery config
    if args.security is False:
        status, response = papi.search_property_by_name(config1)
        if status != 200:
            sys.exit(logger.error(response))

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
                    _, stg_version, prd_version = papi.property_version(response)
                    left = stg_version
                    right = prd_version
                else:
                    sys.exit(logger.error(print_json(data=response)))

            if left == right:
                sys.exit(logger.error('Same version, nothing to compare'))
            else:
                v1 = delivery_config_json(papi, config1, left, args.remove_tag, logger=logger)
                v2 = delivery_config_json(papi, config1, right, args.remove_tag, logger=logger)

        if config2:
            if not all([left, right]):
                sys.exit(logger.error('Missing --left and --right argument and integer value of version'))
            # this value will override config1.v2 when --config2 is provided
            print()
            v1 = delivery_config_json(papi, config1, left, args.remove_tag, logger=logger)
            v2 = delivery_config_json(papi, config2, right, args.remove_tag, logger=logger)

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
                    if args.namecontains:
                        df = pd.DataFrame(resp_1)
                        df = df[df['name'].str.contains(args.namecontains, case=False)].copy()

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
                    logger.critical('no production version, use latest version instead')
                    prd_version = response['latestVersion']

                left = stg_version
                right = prd_version
                if left == right:
                    sys.exit(logger.error(f'Same version {left}, nothing to compare'))

            v1 = security_config_json(appsec, waf_config_name, config1, left, args.remove_tag, logger=logger)
            v2 = security_config_json(appsec, waf_config_name, config1, right, args.remove_tag, logger=logger)

        if config2:
            if not all([left, right]):
                sys.exit(logger.error('Missing --left and --right argument and integer value of version'))
            v1 = security_config_json(appsec, waf_config_name, config1, left, args.remove_tag)
            v2 = security_config_json(appsec, waf_config_name, config2, right, args.remove_tag)

    title = 'Compare report in HTML format is saved at: '
    if args.json is True:
        print()
        logger.warning('Comparing JSON configuration')
        json_index_html = compare_versions(v1, v2, 'index_json', args, logger=logger)
        if json_index_html:
            if not args.no_show:
                webbrowser.open(f'file://{os.path.abspath(json_index_html)}')
            else:
                logger.info(f'{title}{os.path.abspath(json_index_html)}')

    if args.xml is True:
        print()
        logger.warning('Comparing XML Metadata')
        if args.security is False:
            logger.debug(f'{papi.property_id} {papi.asset_id} {papi.group_id}')
            v1_xml = papi.get_properties_version_metadata_xml(config1, papi.asset_id, papi.group_id, left, args.remove_tag)
            v2_xml = papi.get_properties_version_metadata_xml(config1, papi.asset_id, papi.group_id, right, args.remove_tag)
            xml_index_html = compare_versions(v1_xml, v2_xml, 'index_delivery', args, logger=logger)
            if xml_index_html:
                if not args.no_show:
                    webbrowser.open(f'file://{os.path.abspath(xml_index_html)}')
                else:
                    logger.info(f'{title}{os.path.abspath(xml_index_html)}')

        if args.security is True:
            v1_xml = appsec.get_config_version_metadata_xml(waf_config_name, left)
            v2_xml = appsec.get_config_version_metadata_xml(waf_config_name, right)

            v1 = v1_xml['portalWaf']
            v2 = v2_xml['portalWaf']
            xml_portalWaf_index_html = compare_versions(v1, v2, 'index_xml_portalWaf', args, logger=logger)
            if xml_portalWaf_index_html:
                if not args.no_show:
                    webbrowser.open(f'file://{os.path.abspath(xml_portalWaf_index_html)}')
                else:
                    logger.info(f'{title}{os.path.abspath(xml_portalWaf_index_html)}')

            v1 = v1_xml['wafAfter']
            v2 = v2_xml['wafAfter']
            xml_wafAfter_index_html = compare_versions(v1, v2, 'index_xml_wafAfter', args, logger=logger)
            if xml_wafAfter_index_html:
                if not args.no_show:
                    webbrowser.open(f'file://{os.path.abspath(xml_wafAfter_index_html)}')
                else:
                    logger.info(f'{title}{os.path.abspath(xml_wafAfter_index_html)}')


def compare_delivery_behaviors(args, logger):
    '''
    akamai util diff behavior --property AAA BBB CCC \
        --network latest \
        --remove-tags advanced uuid variables templateUuid templateLink xml \
        --behavior allHttpInCacheHierarchy allowDelete allowOptions allowPatch allowPost
    '''
    if args.rulenotcontains and args.rulecontains:
        sys.exit(logger.error('Please use either --rulecontains or --rulenotcontains, not both'))

    properties, left, right = sorted(args.property), args.left, args.right
    account_switch_key, section, edgerc = args.account_switch_key, args.section, args.edgerc
    papi = Papi(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
    has_contracts = papi.get_contracts()
    if isinstance(has_contracts, dict) and has_contracts['status'] == 403:
        sys.exit(logger.error(has_contracts['detail']))
    papi_rules = p.PapiWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc, logger=logger)
    print()

    filename = args.output if args.output else f'{args.property[0]}_compare.xlsx'  # by default use the first property
    filepath = f'output/{filename}'
    prop = {}
    sheet = {}
    all_properties = []

    for i, property in enumerate(properties):
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.error(f'{resp}')

        if not (left and right):
            v_latest, v_staging, v_production = papi.property_version(resp)
            if args.network == 'latest':
                version = v_latest
            elif args.network == 'staging':
                version = v_staging
            else:
                version = v_production

            logger.debug(f'{papi.property_id} {version}')
            ruletree_status, json = papi.property_ruletree(papi.property_id, version, args.remove_tag)
        else:
            if left and i == 0:
                version = left
                ruletree_status, json = papi.property_ruletree(papi.property_id, version, args.remove_tags)
            if right and i == 1:
                version = right
                ruletree_status, json = papi.property_ruletree(papi.property_id, version, args.remove_tags)

        if ruletree_status == 200:
            property_name = f'{property}_v{version}'
            logger.debug(f'{property:<50} {papi.property_id} {property_name}')
            _ = papi.get_property_version_detail(papi.property_id, version)  # show direct url to config on ACC
            all_properties.append(property_name)
            prop[property_name] = json['rules']

    all_behaviors = []
    all_criteria = []
    all_criteria_condition = []
    for property_name, rule in prop.items():
        all_behaviors.append(papi_rules.collect_property_behavior(property_name, rule))
        all_criteria.append(papi_rules.collect_property_criteria(property_name, rule))
        all_criteria_condition.append(papi_rules.collect_property_criteria_condition(property_name, rule))

    db = pd.concat(all_behaviors)
    if args.behavior:
        db = db[db['name'].isin(args.behavior)].copy()
        db = db.reset_index(drop=True)
    sheet['behavior'] = db

    dc = pd.concat(all_criteria)
    if args.criteria:
        dc = dc[dc['name'].isin(args.criteria)].copy()
        dc = dc.reset_index(drop=True)
    sheet['criteria'] = dc

    dcon = pd.concat(all_criteria)
    if args.condition:
        dcon['json_or_xml'] = dcon['json_or_xml'].apply(str)
        dcon = dcon[dcon['json_or_xml'].str.contains(args.condition, na=False)].copy()
        dcon = dcon.reset_index(drop=True)
    sheet['condition'] = dcon

    dcc = pd.concat(all_criteria_condition)

    df = pd.concat([db, dc, dcc])
    # drop if no value in custom_behaviorId for all rows
    df['custom_behaviorId'] = df['custom_behaviorId'].replace('', pd.NA)
    if df['custom_behaviorId'].isna().all():
        del df['custom_behaviorId']

    df['character_count'] = df['json_or_xml'].str.len()
    if df.query('character_count > 32767').empty:
        del df['character_count']
    else:
        print()
        logger.warning('Result contains character over the limit of 32,767 per cell')

    print()
    if df.empty:
        logger.info('no result found')
    else:
        columns = ['property', 'path', 'jsonpath', 'type', 'name']
        if 'custom_behaviorId' in df.columns:
            columns.append('custom_behaviorId')
        if 'character_count' in df.columns:
            columns.append('character_count')
        columns.append('json_or_xml')

        df['path_order'] = df['path'].str.replace(r'\s*\[\s*\d+\s*\]$', '', regex=True)
        df = df.sort_values(by=['property', 'path_order', 'type', 'path'], ascending=[True, True, False, True])
        df = df.reset_index(drop=True)
        logger.debug(df[['path', 'type', 'path_order']])

        sheet['flat_json'] = df[columns]
        # Pivot the DataFrame
        '''
        pivot_df = df.groupby(['type', 'name', 'json', 'path', 'property']).size().unstack(fill_value=0)
        pivot_df = pivot_df.reset_index().rename_axis(None, axis=1)
        sheet['summary'] = pivot_df
        '''

        path_n_behavior = []

        if args.criteria and 'path' in args.criteria:
            path = dc.query("name == 'path'").copy()
            path = path.reset_index(drop=True)
            if not path.empty:
                expanded = pd.json_normalize(path['json_or_xml']).rename(columns=lambda x: x.replace('.', '_'))
                path = path.reset_index(drop=True)
                expanded = expanded.reset_index(drop=True)
                path_df = pd.concat([path.drop(columns='json_or_xml'), expanded], axis=1)
                path = path_df.copy()
                path['type'] = 'path_match'
                path = path.reset_index(drop=True)

            if not path.empty:
                columns = ['property', 'path', 'jsonpath', 'type', 'values']
                path_df = path[columns].copy()
                path_df['path_match_count'] = path_df['values'].str.len()
                path_df['values'] = path_df[['values']].apply(lambda x: dataframe.split_elements_newline(x[0]) if len(x[0]) > 0 else '', axis=1)
                path_n_behavior.append(path_df)

        if args.behavior and 'origin' in args.behavior:
            origin = db.query("name == 'origin'").copy()
            origin = origin.reset_index(drop=True)
            logger.debug(f'\n{origin}')

            if not origin.empty:
                expanded = pd.json_normalize(origin['json_or_xml']).rename(columns=lambda x: x.replace('.', '_'))
                origin = origin.reset_index(drop=True)
                expanded = expanded.reset_index(drop=True)
                origin_df = pd.concat([origin.drop(columns='json_or_xml'), expanded], axis=1)
                origin = origin_df.query("originType == 'CUSTOMER'").copy()
                origin['type'] = 'origin_behavior'
                origin = origin.reset_index(drop=True)
                logger.debug(f'{origin.columns=}')

            if not origin.empty:
                columns = ['property', 'path', 'type', 'hostname']
                origin_df = origin[columns].copy()
                origin_df['path_match_count'] = 0  # set header columns to match path_df
                origin_df = origin_df.rename(columns={'hostname': 'values'})
                path_n_behavior.append(origin_df)

        if len(path_n_behavior) == 2:
            original = pd.concat(path_n_behavior).sort_values(by=['path', 'property', 'type'], ascending=[True, True, False])
            excluded_rules = args.rulenotcontains if args.rulenotcontains else None
            included_rules = args.rulecontains if args.rulecontains else None

            original = original.rename(columns={'path': 'rulename'})

            # excluded_rules = ['Non Russia', 'Kasada', 'CiC Dev', 'Custom Bot', 'Sport and User Services Alternate', 'Pre-Prod']
            if excluded_rules:
                logger.critical(f'{excluded_rules=}')
                excluded = original[original['rulename'].str.contains('|'.join(excluded_rules))].copy()
                included = original[~original['rulename'].str.contains('|'.join(excluded_rules))].copy()

            # included_rules = ['API']
            if included_rules:
                logger.critical(f'{included_rules=}')
                included = original[original['rulename'].str.contains('|'.join(included_rules))].copy()
                excluded = original[~original['rulename'].str.contains('|'.join(included_rules))].copy()

            original_pivot = original.pivot_table(index=['property', 'rulename'], columns='type', values='values', aggfunc='first')
            columns = ['property', 'path_match_count', 'path_match', 'origin_behavior', 'rulename']

            # filter rule contains
            try:
                included_pivot = included.pivot_table(index=['property', 'rulename'], columns='type', values='values', aggfunc='first')
                included_pivot = included_pivot.reset_index()
                included = included.rename(columns={'values': 'path_match'})
                included_pivot = included_pivot.merge(
                    included[['property', 'rulename', 'path_match', 'path_match_count']],
                    on=['property', 'rulename', 'path_match'],
                    suffixes=('', '_included'))
                included = included.reset_index(drop=True)
                included_pivot.columns.name = None
                sheet['included_mod'] = included_pivot[columns]
                included = included.rename(columns={'path_match': 'values'})
                sheet['included_rules'] = included
            except Exception as e:
                msg = '--rulecontains'
                logger.info(f'{msg:<20} not provided')

            # filter rule not contains
            try:
                excluded_pivot = excluded.pivot_table(index=['property', 'rulename'], columns='type', values='values', aggfunc='first')
                excluded_pivot = excluded_pivot.reset_index()
                excluded = excluded.rename(columns={'values': 'path_match'})
                excluded_pivot = excluded_pivot.merge(
                    excluded[['property', 'rulename', 'path_match', 'path_match_count']],
                    on=['property', 'rulename', 'path_match'],
                    suffixes=('', '_included'))
                excluded = excluded.reset_index(drop=True)
                excluded_pivot.columns.name = None
                sheet['excluded_mod'] = excluded_pivot[columns]
                excluded = excluded.rename(columns={'path_match': 'values'})
                sheet['excluded_rules'] = excluded
            except Exception as e:
                msg = '--rulenotcontains'
                logger.info(f'{msg:<20} not provided')

            original_pivot = original_pivot.reset_index()
            original = original.rename(columns={'values': 'path_match'})
            original_pivot = original_pivot.merge(
                original[['property', 'rulename', 'path_match', 'path_match_count']],
                on=['property', 'rulename', 'path_match'],
                suffixes=('', '_included'))
            original = original.reset_index(drop=True)
            original_pivot.columns.name = None
            sheet['original_mod'] = original_pivot[columns]

            original = original.rename(columns={'path_match': 'values'})
            sheet['original_rules'] = original

        files.write_xlsx(filepath, sheet, show_index=False, adjust_column_width=True, freeze_column=4)
        files.open_excel_application(filepath, not args.no_show, sheet['flat_json'])
