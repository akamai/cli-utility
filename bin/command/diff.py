from __future__ import annotations

import os
import subprocess
import sys
import webbrowser
from pathlib import Path

import pandas as pd
from akamai_api.appsec import Appsec
from akamai_api.papi import Papi
from tabulate import tabulate
from utils import diff_html as compare
from utils import files
from utils._logging import setup_logger

logger = setup_logger()


def collect_json(config_name: str, version: int, response_json):
    Path('output').mkdir(parents=True, exist_ok=True)
    config_name = config_name.replace(' ', '_')
    json_file = f'output/{config_name}_v{version}.json'
    logger.debug(json_file)
    files.write_json(json_file, response_json)
    return json_file


def main(args):
    config1 = args.config1
    config2 = args.config2
    left = args.left
    right = args.right
    papi = Papi(account_switch_key=args.account_switch_key)
    appsec = Appsec(account_switch_key=args.account_switch_key)
    logger.debug(f'{config1=} {config2=}')
    if config1:
        if args.kind == 'security':
            status_code, response = appsec.get_config_detail(config1)
            if status_code == 200:
                waf_config_name = response['name']
            elif status_code in [400, 403]:
                status_code, x = appsec.list_waf_configs()
                if status_code == 200:
                    df = pd.DataFrame(x)
                    print(tabulate(df[['name', 'id']], headers='keys', tablefmt='psql', showindex=False))
                    sys.exit(logger.error(f'Security config id {config1} not found, please review spelling from table shown'))
        else:
            status_code, response = papi.search_property_by_name(config1)

    if config1 and config2:
        if not all([left, right]):
            sys.exit(logger.error('Missing --left and --right argument and integer value of version'))

        if status_code == 200:
            left_status_code, ruletree_response = papi.property_ruletree(papi.property_id, left, args.remove_tags)
        if left_status_code == 200:
            v1 = collect_json(config1, left, ruletree_response)

        status_code, response = papi.search_property_by_name(config2)
        if status_code == 200:
            right_status_code, ruletree_response = papi.property_ruletree(papi.property_id, right, args.remove_tags)
        if right_status_code == 200:
            v2 = collect_json(config2, right, ruletree_response)

    if config1 and not config2:
        if status_code == 200:
            if args.kind == 'security':
                pass
            else:
                property_id = papi.property_id

            if left is None and right:
                sys.exit(logger.error('Missing --left argument and integer value of version'))
            elif right is None and left:
                sys.exit(logger.error('Missing --right argument and integer value of version'))
            elif all([left, right]):
                if args.kind == 'security':
                    status_code, sec_response = appsec.get_config_version_detail(config1, left, args.remove_tags)
                    if status_code == 200:
                        v1 = collect_json(f'{config1}_{waf_config_name}', left, sec_response)
                    status_code, sec_response = appsec.get_config_version_detail(config1, right, args.remove_tags)
                    if status_code == 200:
                        v2 = collect_json(f'{config1}_{waf_config_name}', right, sec_response)

                else:
                    status_code, ruletree_response = papi.property_rule_tree(property_id, left, args.remove_tags)
                    if status_code == 200:
                        v1 = collect_json(config1, left, ruletree_response)
                    status_code, ruletree_response = papi.property_ruletree(property_id, right, args.remove_tags)
                    if status_code == 200:
                        v2 = collect_json(config1, right, ruletree_response)
            else:
                if args.kind == 'security':
                    try:
                        stg_version = response['stagingVersion']
                    except:
                        stg_version = 0

                    try:
                        prd_version = response['productionVersion']
                    except:
                        prd_version = 0
                else:
                    stg_version, prd_version = papi.property_version(response)

                if stg_version == prd_version:
                    sys.exit(logger.error('Same version, nothing to compare'))
                else:
                    status_code, ruletree_response = papi.property_ruletree(property_id, stg_version, args.remove_tags)
                    if status_code == 200:
                        v1 = collect_json(config1, stg_version, ruletree_response)
                    status_code, ruletree_response = papi.property_ruletree(property_id, prd_version, args.remove_tags)
                    if status_code == 200:
                        v2 = collect_json(config1, prd_version, ruletree_response)

    logger.debug(f'{v1} {v2}')
    cmd_text = f'diff -u {v1} {v2} | ydiff -s --wrap -p cat'
    subprocess.run(cmd_text, shell=True)
    output_path = 'output/index.html'
    compare.main(v1, v2, output_path, args)

    if not args.no_show:
        webbrowser.open(f'file://{os.path.abspath(output_path)}')

    if args.remove_tags:
        logger.info(f'tags removed from comparison {args.remove_tags}')
