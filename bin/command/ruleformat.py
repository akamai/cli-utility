from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from akamai_api.papi import Papi
from akamai_utils.papi import PapiWrapper
from rich import print_json
from tabulate import tabulate
from utils import files
from utils._logging import setup_logger

logger = setup_logger()
cwd = Path().absolute()


def list_ruleformat() -> list:
    papi = Papi()
    _, formats = papi.list_ruleformat()
    if 'latest' in formats:
        formats = formats[-1:] + formats[:-1]
    logger.info(f'Found {len(formats)} rule formats')
    print(*formats, sep='\n')
    return formats


def get_all_ruleformat_schema(args, papi, formats):
    for format in formats:
        status_code, rule_dict = papi.get_ruleformat_schema(args.product_id, format)
        if status_code == 200:
            if args.save:
                directory = f'output/ruleformat/{args.product_id}'
                Path(directory).mkdir(parents=True, exist_ok=True)
                local_file = f'{directory}/{args.product_id}_{format}.json'
                files.write_json(local_file, rule_dict)
        elif status_code == 404:
            sys.exit(logger.error(f'Invalid product id {args.product_id}'))

    if args.save and directory:
        logger.info(f'Rule formats downloaded in directory: {cwd}/{directory}')


def get_ruleformat_schema(args):
    papi = Papi()
    if args.version is None:
        answer = input('Would you like to see all versions? (y/N)\t')
        if answer.upper() == 'Y':
            formats = list_ruleformat()
            if args.save:
                get_all_ruleformat_schema(args, papi, formats)
            sys.exit(1)
        else:
            logger.info('add --version to get specific version only')
            sys.exit(1)

    status_code, rule_dict = papi.get_ruleformat_schema(args.product_id, args.version)
    if status_code == 200:
        if args.save:
            Path('output/ruleformat').mkdir(parents=True, exist_ok=True)
            local_file = f'output/ruleformat/{args.product_id}_{args.version}.json'
            files.write_json(local_file, rule_dict)
            logger.info(f'Rule format downloaded: {cwd}/{local_file}')
        if args.json:
            print_json(data=rule_dict)
        if args.xlsx:
            papi_wrapper = PapiWrapper()
            displayed_columns = ['type', 'default', 'enum', 'minimum', 'maximum', 'items', 'maxItems', '$ref']
            if args.behavior:
                options = papi_wrapper.get_behavior(rule_dict, behavior=args.behavior)
                df = pd.DataFrame.from_dict(options, orient='index')
                df = df.fillna('')
                behavior_options = list(df.columns)

                diff = list(set(behavior_options) - set(displayed_columns))
                if len(diff) > 0:
                    sys.exit(logger.error(f'{diff} not in chosen column'))
                columns = sorted(list(set(behavior_options).intersection(displayed_columns)), reverse=True)
                df = df[columns]
                if 'type' in df.columns:
                    df = df.rename_axis('options').sort_values(by=['options', 'type'], ascending=[True, False])

                if df.empty:
                    logger.info('do information available')
                else:
                    width = []
                    for x in columns:
                        if x in ['enum', 'items', 'maxItems', 'default']:
                            width.append(30)
                        else:
                            width.append(None)
                    width.insert(0, None)
                    print(tabulate(df, headers='keys', tablefmt='grid', numalign='center', maxcolwidths=width))

            else:
                all_behaviors = list(rule_dict['definitions']['catalog']['behaviors'].keys())
                for behavior in all_behaviors:
                    options = papi_wrapper.get_behavior(rule_dict, behavior)
                    df = pd.DataFrame.from_dict(options, orient='index')
                    df = df.fillna('')

                    behavior_options = list(df.columns)
                    diff = list(set(behavior_options) - set(displayed_columns))
                    if len(diff) > 0:
                        sys.exit(logger.error(f'{diff} not in chosen column'))
                    columns = sorted(list(set(behavior_options).intersection(displayed_columns)), reverse=True)
                    df = df[columns]
                    if 'type' in df.columns:
                        df = df.rename_axis('options').sort_values(by=['options', 'type'], ascending=[True, False])

                    if df.empty:
                        logger.error('do information available')
                    else:
                        width = []
                        for x in columns:
                            if x in ['enum', 'items', 'maxItems', 'default']:
                                width.append(30)
                            else:
                                width.append(None)
                        width.insert(0, None)
                        print(tabulate(df, headers='keys', tablefmt='grid', numalign='center', maxcolwidths=width))

    elif status_code == 404:
        logger.error(f'version {args.version} not found')
        list_ruleformat()
    else:
        print_json(data=rule_dict)
        logger.error('please review error')
