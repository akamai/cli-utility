from __future__ import annotations

import platform
import subprocess
import sys
from pathlib import Path

import pandas as pd
from akamai_api.papi import Papi
from akamai_utils.papi import PapiWrapper
from rich import print_json
from tabulate import tabulate
from utils import _logging as lg
from utils import files

logger = lg.setup_logger()
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
            if args.xlsx:
                directory = f'output/ruleformat/{args.product_id}'
                Path(directory).mkdir(parents=True, exist_ok=True)
                local_file = f'{directory}/{args.product_id}_{format}.json'
                files.write_json(local_file, rule_dict)
        elif status_code == 404:
            sys.exit(logger.error(f'Invalid product id {args.product_id}'))

    if args.xlsx and directory:
        logger.info(f'Rule formats downloaded in directory: {cwd}/{directory}')


def get_ruleformat_schema(args):
    papi = Papi()
    if args.version is None:
        answer = input('Would you like to see all versions? (y/N)\t')
        if answer.upper() == 'Y':
            formats = list_ruleformat()
            if args.xlsx:
                get_all_ruleformat_schema(args, papi, formats)
            sys.exit(1)
        else:
            logger.info('add --version to get specific version only')
            sys.exit(1)

    status_code, rule_dict = papi.get_ruleformat_schema(args.product_id, args.version)
    if status_code == 200:
        papi_wrapper = PapiWrapper()
        if args.json and not args.behavior:
            print_json(data=rule_dict)

        displayed_columns = ['type', 'default', 'enum', 'minimum', 'maximum', 'items', 'maxItems', '$ref']
        if not args.behavior:
            all_behaviors = list(rule_dict['definitions']['catalog']['behaviors'].keys())
        else:
            behaviors = [tag for tag in args.behavior]

            all_data = []
            for behavior in behaviors:
                tmp_data = papi_wrapper.get_behavior(rule_dict, behavior=behavior)
                all_data.append(tmp_data)

            all_behaviors = []
            for data in all_data:
                all_behaviors.extend(list(data.keys()))

        sheets = {}
        for behavior in all_behaviors:
            data = papi_wrapper.get_behavior(rule_dict, behavior)
            options = data[behavior]['properties']['options']['properties']
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
                logger.error(f'{behavior:<50} no information available')
            else:
                width = []
                for x in columns:
                    if x in ['enum', 'items', 'maxItems', 'default']:
                        width.append(30)
                    else:
                        width.append(None)
                width.insert(0, None)
                if not df.empty:
                    logger.warning(behavior)
                    df.index.name = behavior
                    print(tabulate(df, headers='keys', tablefmt='grid', numalign='center', showindex='always', maxcolwidths=width))
                    print()
                    updated_behavior = files.prepare_excel_sheetname(behavior)
                    df = df.reset_index()
                    sheets[updated_behavior] = df

            # df = pd.DataFrame(all_behaviors, columns = ['behavior'])
            # print(tabulate(df, headers='keys', tablefmt='github', numalign='center'))

        if args.xlsx:
            filepath = 'output/ruleformat.xlsx'
            toc = list(sheets.keys())
            toc_dict = {}
            if len(toc) > 5:
                df = pd.DataFrame(toc, columns=['behaviors'])
                df['behaviors'] = df.apply(lambda x: files.make_xlsx_hyperlink_to_another_sheet('ruleformat.xlsx', x['behaviors'], 'A1'), axis=1)
                toc_dict['TOC'] = df
            sheets = {**toc_dict, **sheets}
            files.write_xlsx(filepath, dict_value=sheets, freeze_column=0, show_index=True)
            logger.debug(f'{all_behaviors=}')

            if platform.system() != 'Darwin':
                logger.info('--show argument is supported only on Mac OS')
            else:
                subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])

        # save JSON regardless
        Path('output/ruleformat').mkdir(parents=True, exist_ok=True)
        local_file = f'output/ruleformat/{args.product_id}_{args.version}.json'
        files.write_json(local_file, rule_dict)
        logger.info(f'Rule JSON format downloaded: {cwd}/{local_file}')

    elif status_code == 404:
        logger.error(f'version {args.version} not found')
        list_ruleformat()
    else:
        print_json(data=rule_dict)
        logger.error('please review error')
