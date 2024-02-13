from __future__ import annotations

import sys

import pandas as pd
from akamai_utils import cpcode as cp
from pandarallel import pandarallel
from rich import print_json
from utils import files


def list_cpcode(args, account_folder, logger):
    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key,
                           section=args.section,
                           edgerc=args.edgerc,
                           logger=logger)
    result = []

    if args.product:
        cpc._params['productId'] = args.product
        result.extend(cpc.list_cpcode()['cpcodes'])

    if args.contract:
        for contract in args.contract:
            cpc._params['contractId'] = contract
            result.extend(cpc.list_cpcode()['cpcodes'])

    if args.exactname:
        for name in args.exactname:
            cpc._params['cpcodeName'] = name
            result.extend(cpc.list_cpcode()['cpcodes'])

    if args.product is None and args.contract is None:
        result.extend(cpc.list_cpcode()['cpcodes'])

    if len(result) == 0:
        sys.exit(logger.info('no cpcode found'))

    logger.info(f'Total cpcodes: {len(result)}')
    df = pd.DataFrame(result)
    df.index = df.index + 1
    df['product_names'] = df['products'].apply(lambda x: [item['productName'] for item in x])
    columns = ['cpcodeId', 'cpcodeName', 'contracts', 'product_names', 'accountId']

    sheet = {}
    sheet['all_cpcodes'] = df[columns]
    filepath = f'{account_folder}/all_cpcodes.xlsx'
    files.write_xlsx(filepath, sheet, freeze_column=1) if not df.empty else None
    files.open_excel_application(filepath, True, df)


def list_reporting_group(args, account_folder, logger):
    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key,
                           section=args.section,
                           edgerc=args.edgerc,
                           logger=logger)
    df = pd.DataFrame()
    if args.id:
        result = []
        for ids in args.id:
            resp = cpc.get_reporting_group(int(ids))
            if resp.ok:
                result.append(resp.json())
            else:
                logger.error(f"{ids=} {resp.json()['details']}")
        if len(result) > 0:
            df = pd.DataFrame(result)
    else:
        resp = cpc.list_reporting_group()
        if resp.ok:
            df = pd.DataFrame(resp.json()['groups'])
        else:
            sys.exit(logger.error(print_json(data=resp.json())))

    if not df.empty:
        if args.product:
            pandarallel.initialize(progress_bar=False, verbose=0)
            df['resp'] = df['reportingGroupId'].parallel_apply(lambda x: cpc.list_product_of_reporting_group(x))
            df['products'] = df['resp'].apply(lambda x: x.json()['products'] if x.ok else None)
            df['product_names'] = df['products'].apply(lambda x: [item['productName'] for item in x])
            df.drop(columns=['resp', 'products'], inplace=True)

        sheet = {}
        sheet['reporting_groups'] = df
        filepath = f'{account_folder}/reporting_groups.xlsx'
        files.write_xlsx(filepath, sheet, freeze_column=1) if not df.empty else None
        files.open_excel_application(filepath, True, df)


def delete_reporting_group(args, logger):
    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key,
                           section=args.section,
                           edgerc=args.edgerc,
                           logger=logger)
    for reporting_group_id in args.id:
        resp = cpc.delete_reporting_group(reporting_group_id)
        if resp.ok:
            logger.info(f'{reporting_group_id=} delete successfully.')
        else:
            logger.error(f"{reporting_group_id=} {resp.json()['details']}")


def construct_create_payload(contract, group: int, cp_codes: list, reportingName: str) -> dict:
    result = {}
    result['accessGroup'] = {'contractId': contract, 'groupId': group}

    contracts = []
    contracts.append({'contractId': contract, 'cpcodes': cp_codes})
    result['contracts'] = contracts
    result['reportingGroupName'] = reportingName
    return result


def create_reporting_group(args, logger):
    reporting_groups = []

    df = pd.read_excel(args.input, index_col=0, sheet_name='reporting')
    groups = df.groupby(['contractId', 'groupId'])

    for ids, group_data in groups:
        contract = ids[0]
        group = int(ids[1])
        cp_codes = [{'cpcodeId': int(cp)} for cp in group_data['cpCode']]
        reporting_groupname = group_data['reportingName'].iloc[0]
        if args.group:
            if str(ids[1]) in args.group:
                result = construct_create_payload(contract, group, cp_codes, reporting_groupname)
                reporting_groups.append(result)
        else:
            result = construct_create_payload(contract, group, cp_codes, reporting_groupname)
            reporting_groups.append(result)

    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key,
                           section=args.section,
                           edgerc=args.edgerc,
                           logger=logger)

    for i, payload in enumerate(reporting_groups, start=1):
        resp = cpc.create_reporting_group(payload)
        if not resp.ok:
            error = resp.json()['details'][0]['message']
            logger.error(f"{i:<4} {payload['reportingGroupName']} {error}")
        else:
            logger.info(f"{i:<4} {payload['reportingGroupName']:<50} reportingGroupId {resp.json()['reportingGroupId']} created")


def update_reporting_group(args, logger):
    ...
