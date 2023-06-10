from __future__ import annotations

import platform
import subprocess
import sys
from pathlib import Path

import numpy as np
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

        behaviors_in_catalog = list(rule_dict['definitions']['catalog']['behaviors'].keys())
        bic = list({v.lower(): v for v in behaviors_in_catalog}.keys())

        behaviors_tmp = sorted(rule_dict['definitions']['behavior']['allOf'][0]['properties']['name']['enum'])
        behaviors = list({v.lower(): v for v in behaviors_tmp}.keys())

        notin_catalog_temp = sorted(list(set(behaviors) - set(bic)))
        notin_all_temp = sorted(list(set(bic) - set(behaviors)))

        notin_catalog = list({v.lower(): v for v in notin_catalog_temp}.keys())
        notin_all = list({v.lower(): v for v in notin_all_temp}.keys())

        max_length = max(len(behaviors), len(behaviors_in_catalog), len(notin_catalog), len(notin_all))
        behaviors_df = pd.DataFrame({
            'all': behaviors + [''] * (max_length - len(behaviors)),
            'has_catalog': behaviors_in_catalog + [''] * (max_length - len(behaviors_in_catalog)),
            'notin_catalog': notin_catalog + [None] * (max_length - len(notin_catalog)),
            'notin_all': notin_all + [None] * (max_length - len(notin_all)),

        })

        if args.behavior:
            mask = np.column_stack([behaviors_df[col].astype(str).str.contains('|'.join(args.behavior), na=False) for col in behaviors_df])
            behaviors_df = behaviors_df.loc[mask.any(axis=1)]
            behaviors_df = behaviors_df.reset_index(drop=True)

        print()
        if not behaviors_df.empty:
            print(tabulate(behaviors_df, headers=['all', 'has_catalog', 'notin_catalog', 'notin_all'], tablefmt='simple', showindex='always'))
        else:
            logger.info(f'{args.behavior} not found, try another behavior')

        print()
        if args.nameonly is True:
            sys.exit()
        else:
            behaviors = [tag for tag in args.behavior]
            all_data = []
            for behavior in behaviors:
                tmp_data = papi_wrapper.get_behavior(rule_dict, behavior)
                if tmp_data:
                    all_data.append(tmp_data)

            behaviors = []  # get actual behaviors returned because cli only use name contains
            for data in all_data:
                behaviors.extend(list(data.keys()))

        sheets = {}
        for behavior in behaviors:
            behavior_json = papi_wrapper.get_behavior(rule_dict, behavior)
            if data:

                options = papi_wrapper.get_behavior_option(behavior_json, behavior)
                df = pd.DataFrame.from_dict(options, orient='index')
                df = df.fillna('')
                columns = list(df.columns)
                df = df[columns]
                if 'type' in df.columns:
                    df = df.rename_axis('options').sort_values(by=['options', 'type'], ascending=[True, False])
                if df.empty:
                    logger.error(f'{behavior:<50} no information available')
                    print()
                else:
                    width = []
                    for x in columns:
                        if x in ['enum', 'items', 'maxItems', 'default']:
                            width.append(30)
                        else:
                            width.append(None)
                    width.insert(0, None)
                    if not df.empty:
                        print()
                        logger.warning(behavior)
                        df.index.name = behavior
                        print(tabulate(df, headers='keys', tablefmt='grid', numalign='center', showindex='always', maxcolwidths=width))
                        if args.json:
                            behavior_option_json = behavior_json[behavior]['properties']['options']['properties']
                            print_json(data=behavior_option_json)
                        updated_behavior = files.prepare_excel_sheetname(behavior)
                        df = df.reset_index()
                        sheets[updated_behavior] = df

        if args.xlsx and sheets:
            filepath = 'output/ruleformat.xlsx'
            toc = list(sheets.keys())
            toc_dict = {}
            if len(toc) > 5:
                df = pd.DataFrame(toc, columns=['behaviors'])
                df['behaviors'] = df.apply(lambda x: files.make_xlsx_hyperlink_to_another_sheet('ruleformat.xlsx', x['behaviors'], 'A1'), axis=1)
                toc_dict['TOC'] = df
            sheets = {**toc_dict, **sheets}
            files.write_xlsx(filepath, dict_value=sheets, freeze_column=0, show_index=True)
            logger.debug(f'{behaviors=}')

            if platform.system() != 'Darwin':
                logger.info('--show argument is supported only on Mac OS')
            else:
                subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])

        # save JSON regardless
        Path('output/ruleformat').mkdir(parents=True, exist_ok=True)
        local_file = f'output/ruleformat/{args.product_id}_{args.version}.json'
        files.write_json(local_file, rule_dict)
        print()
        logger.info(f'Full rule JSON format downloaded: {cwd}/{local_file}')

    elif status_code == 404:
        logger.error(f'version {args.version} not found')
        list_ruleformat()
    else:
        print_json(data=rule_dict)
        logger.error('please review error')
