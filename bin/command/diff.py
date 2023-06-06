from __future__ import annotations

import os
import subprocess
import sys
import webbrowser
from pathlib import Path

import pandas as pd
from akamai_api.appsec import Appsec
from akamai_api.papi import Papi
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
    json_file = f'output/diff/json/{config_name}_v{version}.json'
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


def main(args):

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
