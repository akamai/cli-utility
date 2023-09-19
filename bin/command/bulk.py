from __future__ import annotations

import ast
import sys
import time

import numpy as np
import pandas as pd
from akamai_utils import cpcode as cp
from akamai_utils import papi as p
from akamai_utils import siteshield as ss
from akamai_utils.papi import PapiWrapper
from pandarallel import pandarallel
from rich import print_json
from tabulate import tabulate
from utils import files
from yaspin import yaspin
from yaspin.spinners import Spinners


def patch_version_result(data: list, extract_columns: list) -> list:
    return [{col: version.get(col) for col in extract_columns} for version in data]


def remove_string_from_list(object: list, string_to_remove: str, logger: None):
    if isinstance(object, list):
        return [item.replace(string_to_remove, '') if isinstance(item, str) else item for item in object]
    else:
        logger.critical(f'Invalid data type.  Expect list, got {type(object)}')


def check_filter_condition(args, df, logger) -> pd.DataFrame:
    print()
    msg = 'original count from bulk search: '
    logger.critical(f'{msg:<40}{df.shape[0]} properties')

    if args.version == 'production':
        df = df.query("productionStatus == 'ACTIVE'")
    if args.version == 'staging':
        df = df.query("stagingStatus == 'ACTIVE'")
    if args.version == 'latest':
        df = df.query('isLatest == True')
    if args.name_contains:
        df = df[df['propertyName'].str.contains(args.name_contains)].copy()
    if args.env:
        df = df.query(f"env == '{args.env}'").copy()

    try:
        if args.property:
            df = df[df['propertyName'].isin(args.property)].copy()
    except:
        pass  # argument does not apply to the subcommand

    try:
        if args.include:
            with open(args.include) as file:
                propertyId = [line.strip() for line in file]
            df = df[df['propertyId'].isin(propertyId)].copy()
    except:
        pass  # argument does not apply to the subcommand

    try:
        if args.exclude:
            with open(args.exclude) as file:
                property_name = [line.strip() for line in file]
            df = df[~df['propertyName'].isin(property_name)].copy()
    except:
        pass  # argument does not apply to the subcommand

    msg = 'with conditions: '
    logger.critical(f'{msg:<40}{df.shape[0]} properties')
    return df


def fetch_status_patch(papi: PapiWrapper, id: int, version_note: str, logger) -> pd.DataFrame:
    resp = papi.list_bulk_patch(id)
    # print_json(data=resp.json())
    if resp.status_code != 200:
        logger.critical(resp.status_code)
        print_json(data=resp.json())
    else:
        result = resp.json()['patchPropertyVersions']
        columns_to_extract = ['propertyName', 'patchPropertyId', 'patchPropertyVersion', 'patchPropertyVersionStatus']
        extract_result = patch_version_result(result, columns_to_extract)
        df = pd.DataFrame(extract_result)

        df['bulkPatchId'] = id
        columns_to_extract.insert(0, 'bulkPatchId')
        df = df.rename(columns={'patchPropertyVersionStatus': 'status'})

        columns_to_extract.remove('patchPropertyVersionStatus')
        columns_to_extract.append('status')
        update_df = df[columns_to_extract].copy()

        pandarallel.initialize(progress_bar=False, nb_workers=4, verbose=0)

        update_df['ruleFormat'] = update_df.parallel_apply(lambda row: papi.get_property_version_detail(row['patchPropertyId'], row['patchPropertyVersion'], 'ruleFormat'), axis=1)
        update_df['assetId'] = update_df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['patchPropertyId'], row['patchPropertyVersion'], 'assetId'), axis=1)
        update_df['groupId'] = update_df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['patchPropertyId'], row['patchPropertyVersion'], 'groupId'), axis=1)
        update_df['url'] = update_df.parallel_apply(lambda row: papi.property_url_edit_version(row['assetId'], row['patchPropertyVersion'], row['groupId']), axis=1)
        columns_to_extract = update_df.columns.tolist()
        columns_to_extract.extend(['ruleFormat', 'url'])
        if version_note:
            update_df['current_rule'] = update_df.parallel_apply(lambda row: papi.get_property_full_ruletree(row['patchPropertyId'], row['patchPropertyVersion']).json(), axis=1)
            update_df['version_status'] = update_df.parallel_apply(lambda row: papi.update_property_ruletree(row['patchPropertyId'],
                                                                                            row['patchPropertyVersion'],
                                                                                            row['ruleFormat'],
                                                                                            row['current_rule']['rules'],
                                                                                            version_note), axis=1)
            if 'version_status' not in columns_to_extract:
                columns_to_extract.append('version_status')
    return update_df


def fetch_status_activation(papi: PapiWrapper, id: int, logger) -> pd.DataFrame:
    resp = papi.list_bulk_activation(id)
    if resp.status_code != 200:
        logger.critical(resp.status_code)
    else:
        # print_json(data=resp.json())
        result = resp.json()['activatePropertyVersions']
        columns_to_extract = ['propertyName', 'propertyId', 'propertyVersion', 'network',
                              'propertyActivationsLink', 'activationStatus', 'taskStatus', 'fatalError']
        extract_result = patch_version_result(result, columns_to_extract)
        df = pd.DataFrame(extract_result)
        df['activationId'] = df['propertyActivationsLink'].str[-8:]
        df['bulkActivationId'] = id
        columns_to_extract.insert(0, 'bulkActivationId')
        columns_to_extract.append('activationId')
        activation_df = df[columns_to_extract].copy()
    return activation_df


def bulk_search(args, account_folder, logger) -> pd.DataFrame:
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
    all_df = []
    if not args.id and not args.jsonpath:
        sys.exit(logger.error('please provide either --id or --jsonpath'))
    pandarallel.initialize(progress_bar=False, nb_workers=4, verbose=0)
    if args.id:
        bulk_search_id = int(args.id)
        resp = papi.list_bulk_search(bulk_search_id)
        if not resp.ok:
            sys.exit(logger.error(f'bulkSearchId {bulk_search_id} not found'))
        else:
            bulk_search_result = resp.json()
            logger.info('bulkSearchQuery:')
            print_json(data=bulk_search_result['bulkSearchQuery'])
            bulk_search_id = bulk_search_result['bulkSearchId']
            logger.critical(f'bulkSearchId: {bulk_search_id}')
            df = pd.DataFrame(bulk_search_result['results'])
            df['bulkSearchId'] = bulk_search_id
            df['contractId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'contractId'), axis=1)
            df['groupId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'groupId'), axis=1)
            all_df.append(df)
    else:
        query = files.load_json(args.jsonpath)
        if args.contract_id and args.group_id:
            sys.exit(logger.error('Please provide either contract-id or group-id, not both'))

        if args.contract_id:
            papi.contract_id = args.contract_id
            resp = papi.bulk_search(query)
            if resp.ok:
                bulk_result = resp.json()
                df = pd.DataFrame(bulk_result['results'])
                bulk_search_id = bulk_result['bulkSearchId']
                df['bulkSearchId'] = bulk_search_id
                df['contractId'] = args.contract_id
                df.loc[:, 'groupId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'groupId'), axis=1)
                all_df.append(df)

        if args.group_id:
            for group_id in args.group_id:
                papi.group_id = group_id
                resp = papi.bulk_search(query)
                if resp.ok:
                    bulk_result = resp.json()
                    df = pd.DataFrame(bulk_result['results'])
                    if df.empty:
                        logger.warning(f'{group_id:<30}       no property found\n')
                        # print_json(data=bulk_result)
                    else:
                        bulk_search_id = bulk_result['bulkSearchId']
                        df['bulkSearchId'] = bulk_search_id
                        df.loc[:, 'contractId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'contractId'), axis=1)
                        df['groupId'] = group_id
                        logger.debug(f'{group_id:<30} {df.shape[0]:<5} properties\n')
                        all_df.append(df)
                else:
                    logger.debug(f'{resp.status_code} {resp.url}')
            if len(all_df) == 0:
                sys.exit()

        if not args.contract_id and not args.group_id:
            # lookup whole account
            resp = papi.bulk_search(query)
            if resp.ok:
                bulk_result = resp.json()
                df = pd.DataFrame(bulk_result['results'])
                bulk_search_id = bulk_result['bulkSearchId']
                df['bulkSearchId'] = bulk_search_id
                df.loc[:, 'contractId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'contractId'), axis=1)
                df.loc[:, 'groupId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'groupId'), axis=1)
                all_df.append(df)

    if len(all_df) == 0:
        sys.exit()

    df = pd.concat(all_df)

    if df.empty:
        sys.exit(logger.info('found nothing'))
    df.loc[:, 'env'] = df.parallel_apply(lambda row: papi.guestimate_env_type(row['propertyName']), axis=1)
    df.loc[:, 'assetId'] = df.parallel_apply(lambda row: papi.get_property_version_full_detail(row['propertyId'], row['propertyVersion'], 'assetId'), axis=1)

    result_df = check_filter_condition(args, df, logger).copy()
    if result_df.empty:
        sys.exit(logger.error('no property found with requested conditions'))
    else:
        result_df.loc[:, 'groupName'] = result_df.parallel_apply(lambda row: papi.get_group_name(row['groupId']), axis=1)
        result_df.loc[:, 'propertyURL'] = result_df.parallel_apply(lambda row: papi.property_url_edit_version(row['assetId'], row['propertyVersion'], row['groupId']), axis=1)
        result_df.loc[:, 'propertyName(hyperlink)'] = result_df.parallel_apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['propertyURL'], row['propertyName']), axis=1)

        result_df.loc[:, 'productId'] = result_df.parallel_apply(lambda row: papi.get_property_version_detail(row['propertyId'], row['propertyVersion'], 'productId'), axis=1)
        result_df.loc[:, 'ruleFormat'] = result_df.parallel_apply(lambda row: papi.get_property_version_detail(row['propertyId'], row['propertyVersion'], 'ruleFormat'), axis=1)

        # TODO need to make this fleixble and not hardcode '/options/strictMode'
        result_df.loc[:, 'matchLocations'] = df['matchLocations'].parallel_apply(lambda x: remove_string_from_list(x, '/options/strictMode', logger=logger))
        result_df = result_df.sort_values(by=['env', 'groupId', 'propertyName'])

        logger.debug(result_df.dtypes)
        result_df = result_df.reset_index(drop=True)

    columns = ['bulkSearchId', 'contractId', 'groupId', 'groupName', 'propertyId', 'env',
               'propertyName(hyperlink)', 'propertyName', 'propertyVersion',
               'productionStatus', 'stagingStatus', 'isLatest', 'isLocked',
               'productId', 'ruleFormat',
               'matchLocations']

    console_columns = ['env', 'groupId', 'propertyName', 'propertyVersion', 'matchLocations', 'propertyURL']
    print(tabulate(result_df[console_columns], headers=console_columns, tablefmt='simple', numalign='center'))
    print()

    sheet = {}
    sheet['search_results'] = result_df[columns]
    if args.output:
        filepath = f'{account_folder}/bulk/{args.output}'
    elif args.tag:
        filepath = f'{account_folder}/bulk/bulk_{args.tag}_search_{bulk_search_id}.xlsx'
    elif bulk_search_id != 0:
        filepath = f'{account_folder}/bulk/bulk_search_{bulk_search_id}.xlsx'
    else:
        filepath = f'{account_folder}/bulk/bulk_search.xlsx'

    files.write_xlsx(filepath, sheet)
    files.open_excel_application(filepath, show=True, df=result_df[columns])

    if args.group_id is None:
        all_groups = result_df.groupId.unique().tolist()
        modified_list = [word for word in all_groups]
        all_groups = ' '.join(modified_list)
        logger.warning(f'--group-id {all_groups}')

    return df


def bulk_create(args, account_folder, logger):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
    pandarallel.initialize(progress_bar=False, nb_workers=4, verbose=0)
    if args.id:
        # only show result from bulk create call
        # !!! this option doesn't provide matchLocations, so output cannot be used for bulk update !!!
        bulk_create_id = int(args.id)
        resp = papi.list_bulk_create(bulk_create_id)
        if resp.ok:
            bulk_create_result = resp.json()
            df = pd.DataFrame(bulk_create_result['createPropertyVersions'])
            df['bulkCreateId'] = bulk_create_id
            df = df.rename(columns={'createFromVersion': 'base_version',
                                    'propertyVersion': 'new_version'})
            print(tabulate(df, headers=df.columns, tablefmt='simple', numalign='center'))
            sheet = {}
            sheet['data'] = df
            if args.tag:
                filepath = f'{account_folder}/bulk/bulk_{args.tag}_create_{bulk_create_id}.xlsx'
            else:
                filepath = f'{account_folder}/bulk/bulk_create_{bulk_create_id}.xlsx'
            files.write_xlsx(filepath, sheet)
            files.open_excel_application(filepath, True, df)
            sys.exit()
        else:
            sys.exit(logger.error(resp.text))
    elif args.input_excel:
        df = pd.read_excel(args.input_excel)
        df['propertyId'] = df['propertyId'].astype(str)
        df['matchLocations'] = df['matchLocations'].apply(ast.literal_eval)  # convert string to list
        if df.empty:
            sys.exit(logger.error('Properties not found'))
    elif args.bulksearchid:
        if args.version is None:
            sys.exit(logger.error('--version must be provided'))
        bulk_search_id = int(args.bulksearchid)
        resp = papi.list_bulk_search(bulk_search_id)
        if not resp.ok:
            sys.exit(logger.error(resp.text))
        else:
            bulk_search_result = resp.json()
            logger.info('bulkSearchQuery:')
            print_json(data=bulk_search_result['bulkSearchQuery'])
            df = pd.DataFrame(bulk_search_result['results'])
            df['bulkSearchId'] = bulk_search_result['bulkSearchId']
            df.loc[:, 'env'] = df.parallel_apply(lambda row: papi.guestimate_env_type(row['propertyName']), axis=1)
            if df.empty:
                sys.exit(logger.error('Properties not found'))

    result_df = check_filter_condition(args, df, logger)
    if result_df.empty:
        sys.exit(logger.info('condition not found'))
    else:
        columns = ['propertyId', 'propertyVersion', 'propertyName', 'productionStatus',
                    'stagingStatus', 'isLatest', 'matchLocations', 'bulkSearchId']
        if 'groupId' in result_df.columns:
            columns.insert(1, 'groupId')
        print()
        print(tabulate(result_df[columns], headers=columns, tablefmt='simple', numalign='center'))

    result_df['property_list'] = result_df.parallel_apply(lambda row: (row['propertyId'], row['propertyVersion']), axis=1)
    result_df = result_df.rename(columns={'propertyVersion': 'old_version'})
    print()
    logger.warning('Rename column propertyVersion to old_version')
    print(tabulate(result_df, headers=result_df.columns, tablefmt='simple', numalign='center'))
    properties = result_df['property_list'].values.tolist()
    logger.debug(properties)

    create_resp = papi.bulk_create_properties(properties)
    if not create_resp.ok:
        sys.exit(logger.error(print_json(data=create_resp.json())))

    bulk_create_id = create_resp.json()['bulkCreateId']
    create_df = pd.DataFrame(create_resp.json()['createPropertyVersions'])
    create_df['bulkCreateId'] = bulk_create_id

    print(tabulate(create_df, headers=create_df.columns, tablefmt='simple', numalign='center'))
    create_df = create_df.rename(columns={'createFromVersion': 'base_version'})
    try:
        create_df = create_df.rename(columns={'propertyVersion': 'new_version'})
    except:
        logger.error('Fail to rename column new_version')

    logger.debug(f'result_df \n {result_df.dtypes}')
    logger.debug(f'create_df \n {create_df.dtypes}')

    merge = pd.merge(result_df, create_df, on='propertyId')
    logger.debug(merge.dtypes)
    logger.debug(f'\n{merge}')

    try:
        # TODO need to make this fleixble and not hardcode '/options/strictMode'
        merge['matchLocations'] = merge['matchLocations'].parallel_apply(lambda x: remove_string_from_list(x, '/options/strictMode', logger))
        selected_columns = ['bulkCreateId', 'env', 'propertyName', 'propertyId', 'base_version',
                            'new_version',
                            'createVersionStatus', 'matchLocations']
    except:
        pass

    if merge.empty:
        print_json(data=create_resp.json())
        sys.exit(logger.error('Merge result_df and create_df fail'))

    try:
        print()
        print(tabulate(merge[selected_columns], headers=selected_columns, tablefmt='simple'))
    except KeyError as err:
        logger.critical(f"'{err}' not found in merge_df")
        print(tabulate(merge, headers=merge.columns, tablefmt='simple'))
        selected_columns = merge.columns.tolist()

    sheet = {}
    sheet['data'] = merge[selected_columns]
    if args.tag:
        filepath = f'{account_folder}/bulk/bulk_{args.tag}_create_{bulk_create_id}.xlsx'
    else:
        filepath = f'{account_folder}/bulk/bulk_create_{bulk_create_id}.xlsx'
    files.write_xlsx(filepath, sheet)
    files.open_excel_application(filepath, True, merge[selected_columns])


def bulk_update(args, account_folder, logger):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
    pandarallel.initialize(progress_bar=False, nb_workers=4, verbose=0)
    version_note = args.version_note
    if args.id:
        # review result of the bulk update
        bulk_patch_id = int(args.id)
        update_df = fetch_status_patch(papi, bulk_patch_id, version_note, logger=logger)
        update_df = update_df.sort_values(by=['status', 'propertyName'])
        update_df = update_df.reset_index(drop=True)
        print(tabulate(update_df, headers=update_df.columns, tablefmt='simple', numalign='center'))

        if update_df.empty:
            sheet = {}
            sheet['update'] = update_df
            if args.tag:
                filepath = f'{account_folder}/bulk/bulk_{args.tag}_update_{bulk_patch_id}.xlsx'
            else:
                filepath = f'{account_folder}/bulk/bulk_update_{bulk_patch_id}.xlsx'
            files.write_xlsx(filepath, sheet)
            files.open_excel_application(filepath, True, update_df)

        sys.exit()

    if args.input_excel:
        # read excel generated from bulk create command to pick up thew new version to update with the new rule
        try:
            df = pd.read_excel(args.input_excel)
        except FileNotFoundError as err:
            print()
            sys.exit(logger.error(err, exc_info=False))

        df['matchLocations'] = df['matchLocations'].apply(ast.literal_eval)
        load_columns = ['propertyId', 'propertyName', 'new_version', 'matchLocations']
        print(tabulate(df[load_columns], headers=load_columns, tablefmt='simple', numalign='center'))

        df['property_list'] = df.parallel_apply(lambda row: (row['propertyId'], row['new_version'], row['matchLocations']), axis=1)
        properties = df['property_list'].values.tolist()
        query = files.load_json(args.jsonpath)
        resp = papi.bulk_update_behavior(properties, query)

        if not resp.ok:
            print_json(data=resp.json())
        else:
            bulk_patch_id = resp.json()['bulkPatchId']
            update_df = fetch_status_patch(papi, bulk_patch_id, version_note, logger=logger)
            print()
            logger.critical(f'Fetch_status_patch {bulk_patch_id=} {update_df.shape[0]=}')
            columns = ['bulkPatchId', 'patchPropertyId', 'url', 'status', 'patchPropertyVersion', 'propertyName']
            print(tabulate(update_df[columns], headers=columns, tablefmt='simple', numalign='center'))

            logger.critical(f'\n>> run akamai onboard bulk update --id {bulk_patch_id} to change status of update')

            '''
            # Too slow let using run bulk update --id instead
            total_properties = update_df.shape[0]
            row_count = update_df.query("(status == 'COMPLETE') | (status == 'SUBMISSION_ERROR')").shape[0]
            columns = ['bulkPatchId', 'patchPropertyId', 'url', 'resp_status', 'status', 'patchPropertyVersion', 'propertyName']
            while row_count < total_properties and not update_df.empty:
                print(tabulate(update_df[columns], headers=columns, tablefmt='simple', numalign='center'))
                update_df = fetch_status_patch(papi, bulk_patch_id, version_note, logger=logger)
                row_count = update_df.query("(status == 'COMPLETE') | (status == 'SUBMISSION_ERROR')").shape[0]
                print()
            print(tabulate(update_df[columns], headers=columns, tablefmt='simple', numalign='center'))
            '''


def bulk_activate(args, account_folder, logger):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
    pandarallel.initialize(progress_bar=False, nb_workers=4, verbose=0)
    if args.id:
        ids = [int(x) for x in args.id]
        combined_activation_result = []
        for activation_id in ids:
            activation = fetch_status_activation(papi, activation_id, logger=logger)
            combined_activation_result.append(activation)
        if combined_activation_result == 0:
            sys.exit(logger.info('found nothing'))
        else:
            activation = pd.concat(combined_activation_result)
            columns = activation.columns.tolist()
            columns.remove('fatalError')
        activation = activation.sort_values(by=['fatalError', 'propertyName'], ascending=[False, True])
        pending = activation.query("(taskStatus != 'COMPLETE')").copy()
        pending = pending.reset_index(drop=True)
        print()
        print(tabulate(pending[columns], headers=columns, tablefmt='simple', numalign='center'))

        summary = activation.groupby(['bulkActivationId', 'network', 'taskStatus'])[['propertyId', 'fatalError']].count()
        logger.info(f'\n\n{summary}')
        sys.exit()
    elif args.input_excel:
        if args.network is None:
            sys.exit(logger.error('--network is required.'))

        try:
            df = pd.read_excel(args.input_excel, dtype=str)
        except FileNotFoundError as err:
            print()
            sys.exit(logger.error(err, exc_info=False))

        total_properties = df.shape[0]
        original_columns = df.columns
        if 'activationId' not in original_columns:
            if args.normal:
                with yaspin() as sp:
                    df['activationId'] = df.parallel_apply(lambda row: papi.activate_property_version(
                        row['propertyId'], row['new_version'], args.network, args.note, args.email, args.review_email), axis=1)
                    df[['network', 'activation_status']] = df.parallel_apply(lambda row: pd.Series(papi.activation_status(
                        row['propertyId'], row['activationId'], int(row['new_version']))), axis=1)

                    columns = ['propertyName', 'propertyId', 'base_version', 'new_version',
                               'activationId', 'network', 'activation_status']

                    sheet = {}
                    sheet['activation'] = df[columns]
                    if args.tag:
                        filepath = f'{account_folder}/bulk/bulk_{args.tag}_activation_{args.network}_normal.xlsx'
                    else:
                        filepath = f'{account_folder}/bulk/bulk_activation_{args.network}_normal.xlsx'
                    files.write_xlsx(filepath, sheet)
                    files.open_excel_application(filepath, False, df[columns])

                with yaspin(Spinners.star, timer=True) as sp:
                    row_count = df.query("activation_status == 'ACTIVE'").shape[0]
                    count = 0
                    while row_count < total_properties and not df.empty:
                        if args.network == 'staging':
                            time.sleep(27)
                        else:
                            time.sleep(57)
                        count += 1
                        print()
                        logger.critical(count)
                        print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))
                        df[['network', 'activation_status']] = df.parallel_apply(lambda row: pd.Series(papi.activation_status(
                            row['propertyId'], row['activationId'], row['new_version'])), axis=1)
                        row_count = df.query("activation_status == 'ACTIVE'").shape[0]

                print()
                print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))
                return 0
            else:
                df['property_list'] = df.parallel_apply(lambda row: (row['propertyId'], row['new_version']), axis=1)
                properties = df['property_list'].values.tolist()
                logger.critical(properties)
                resp = papi.bulk_activate_properties(args.network, args.email, args.review_email, args.note, properties)
                logger.critical(resp.status_code)
                id = int(resp.json()['bulkActivationId'])
                activation = fetch_status_activation(papi, id, logger=logger)
        else:
            df[['network', 'activation_status']] = df.parallel_apply(lambda row: pd.Series(papi.activation_status(
                                                                     row['propertyId'], row['activationId'], int(row['new_version']))), axis=1)
            columns = ['propertyName', 'propertyId', 'base_version', 'new_version', 'activationId', 'network', 'activation_status']
            with yaspin(Spinners.star, timer=True) as sp:
                row_count = df.query("activation_status == 'ACTIVE'").shape[0]
                count = 0
                while row_count < total_properties and not df.empty:
                    if args.network == 'staging':
                        time.sleep(27)
                    else:
                        time.sleep(57)
                    count += 1
                    print()
                    logger.critical(count)
                    print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))
                    df[['network', 'activation_status']] = df.parallel_apply(lambda row: pd.Series(
                        papi.activation_status(row['propertyId'], row['activationId'], row['new_version'])), axis=1)
                    row_count = df.query("activation_status == 'ACTIVE'").shape[0]

            print()
            print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))
            return 0

    total_properties = activation.shape[0]
    row_count = activation.query("(taskStatus == 'COMPLETE') | (taskStatus == 'SUBMISSION_ERROR')").shape[0]
    columns = ['bulkActivationId', 'propertyId', 'propertyVersion', 'network',
               'activationStatus', 'taskStatus', 'activationId', 'propertyName']
    count = 0
    while row_count < total_properties and not activation.empty:
        if args.network == 'staging':
            time.sleep(27)
        else:
            time.sleep(57)
        count += 1
        print()
        logger.critical(count)
        print(tabulate(activation[columns], headers=columns, tablefmt='simple', numalign='center'))
        activation = fetch_status_activation(papi, id, logger=logger)
        row_count = activation.query("(taskStatus == 'COMPLETE') | (taskStatus == 'SUBMISSION_ERROR')").shape[0]

    activation = activation.rename(columns={'propertyVersion': 'new_version'})
    columns = ['bulkActivationId', 'propertyId', 'new_version', 'network',
               'activationId', 'activation_status', 'propertyName']
    activation[['network', 'activation_status']] = activation.parallel_apply(lambda row: pd.Series(papi.activation_status(
        row['propertyId'], row['activationId'], row['new_version'])), axis=1)
    row_count = activation.query("activation_status == 'ACTIVE'").shape[0]

    with yaspin(Spinners.star, timer=True) as sp:
        while row_count < total_properties and not activation.empty:
            if args.network == 'staging':
                time.sleep(27)
            else:
                time.sleep(57)
            count += 1
            print()
            logger.critical(count)
            print(tabulate(activation[columns], headers=columns, tablefmt='simple', numalign='center'))
            activation[['network', 'activation_status']] = activation.apply(lambda row: pd.Series(papi.activation_status(
                row['propertyId'], row['activationId'], row['new_version'])), axis=1)
            row_count = activation.query("activation_status == 'ACTIVE'").shape[0]

    print()
    print(tabulate(activation[columns], headers=columns, tablefmt='simple', numalign='center'))
    if not activation.empty:
        sheet = {}
        sheet['activation'] = activation[columns]
        if args.tag:
            filepath = f'{account_folder}/bulk/bulk_{args.tag}_activation_{args.network}_{id}.xlsx'
        else:
            filepath = f'{account_folder}/bulk/bulk_activation_{args.network}_{id}.xlsx'
        files.write_xlsx(filepath, sheet)
        files.open_excel_application(filepath, False, activation[columns])
    return 0


def bulk_add(args, account_folder, logger):
    """
    add rules to newly create version
    """
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)

    if args.bulk_id:
        bulk_search_id = args.bulk_id
        resp = papi.list_bulk_patch(int(bulk_search_id))
    else:
        patch_json = files.load_json(args.input_json)
        df = pd.read_excel(args.input_excel)
        pandarallel.initialize(progress_bar=False, nb_workers=4, verbose=0)
        df['latest_version'] = df.parallel_apply(lambda row: papi.get_property_version_latest(row['propertyId'])['latestVersion'], axis=1)
        # activate

        df['status'] = df.apply(lambda row:
            papi.activate_property_version(row['propertyId'],
                                           row['latest_version'],
                                        args.network, args.note, args.emails
                                            ), axis=1)
        columns = ['propertyId', 'latest_version', 'status']
        print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))
        # activate

        '''
        # add rule
        df['current_rule'] = df.apply(lambda row: papi.get_property_full_ruletree(row['propertyId'], row['latest_version']).json(), axis=1)
        df['new_rule'] = df.apply(lambda row: papi.build_new_ruletree(row['current_rule'], patch_json), axis=1)
        df['property_list'] = df.apply(lambda row: (row['propertyId'], row['latest_version']), axis=1)

        df['status'] = df.apply(lambda row: papi.update_property_ruletree(row['propertyId'],
                                                                                   row['latest_version'],
                                                                                   row['new_rule']['rules']), axis=1)
        columns = ['propertyId', 'latest_version', 'current_rule', 'status']
        print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))
        '''
