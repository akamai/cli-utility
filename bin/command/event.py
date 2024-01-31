from __future__ import annotations

import sys

import pandas as pd
from akamai_utils import cpcode as cp
from akamai_utils import eventcenter as ec
from pandarallel import pandarallel
from rich import print_json
from tabulate import tabulate
from utils import files


def list_events(args, logger):
    event = ec.EventCenterWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc)
    resp = event.list_events()
    if not resp.ok:
        logger.error(resp.status_code)
        logger.error(resp.json())
    else:
        if len(resp.json()['data']) == 0:
            sys.exit(logger.info('No event center found'))
        else:
            df = pd.DataFrame(resp.json()['data'])
            df = df.sort_values(by='name', key=lambda col: col.str.lower())
            # df = df.sort_values(by='id')
            df = df.reset_index(drop=True)
            logger.debug(df.columns)
            columns = ['tags', 'name', 'id', 'version', 'start', 'end', 'customerEventId']

            print()
            if args.name_contains:
                df = df[df['name'].str.contains(args.name_contains)]
                df = df.reset_index(drop=True)
            print(tabulate(df, headers=columns, tablefmt='simple'))
            all_ids = df.id.values.tolist()
            modified_list = [str(id) for id in all_ids]
            all_ids = ' '.join(modified_list)
            logger.warning(f'--id {all_ids}')


def remove_event(args, logger):
    event = ec.EventCenterWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc)
    for id in args.id:
        remove_resp = event.remove_event(id)
        if remove_resp.ok:
            logger.info(f'{id} removed successfully')
        else:
            print_json(data=remove_resp.json())


def get_event(args, account_folder, logger):
    account_switch_key, section, edgerc = args.account_switch_key, args.section, args.edgerc
    event = ec.EventCenterWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
    cpc = cp.CpCodeWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
    pandarallel.initialize(progress_bar=False, verbose=0)

    if args.id:
        all_ids = args.id
    else:
        resp = event.list_events()
        if resp.ok and len(resp.json()['data']) > 0:
            print()
            df = pd.DataFrame(resp.json()['data'])
            all_ids = df.id.values.tolist()
            args.id = all_ids

    logger.info(f'Processing total event id: {len(all_ids)}')

    df = pd.DataFrame(all_ids, columns=['eventcenter_id'])
    df['resp'] = df['eventcenter_id'].parallel_apply(lambda x: event.get_event(x))
    df['objects'] = df['resp'].parallel_apply(lambda x: x.json()['objects'] if x.ok else '')

    df_exploded = df.explode('objects').reset_index(drop=True)
    df_exploded = pd.concat([df_exploded.drop(['objects'], axis=1), df_exploded['objects'].apply(pd.Series)], axis=1)
    df_exploded = df_exploded[['eventcenter_id', 'name', 'cpCode']]
    df = df_exploded.rename(columns={'name': 'event_name'})
    logger.debug(f'\n{df}')

    df['cpcode_name'] = df['cpCode'].parallel_apply(lambda x: cpc.get_cpcode_name(x))
    df['cpCode'] = df['cpCode'].astype(str)
    df = df.sort_values(by=['event_name', 'cpcode_name'], key=lambda x: x.str.lower())
    df = df.reset_index(drop=True)
    df.index = df.index + 1
    print()
    columns = ['eventcenter_id', 'event_name', 'cpCode', 'cpcode_name']
    print(tabulate(df[columns], headers=columns, tablefmt='simple', numalign='center'))

    sheet = {}
    print()
    sheet['events'] = df[columns]
    filepath = f'{account_folder}/eventcenter.xlsx'
    files.write_xlsx(filepath, sheet, freeze_column=2) if not df.empty else None
    files.open_excel_application(filepath, show=True, df=df)


def create_event(args, logger):
    if args.count and args.frequency is None:
        sys.exit(logger.error('please provide value for --frequency'))

    if args.frequency and args.count is None:
        sys.exit(logger.error('please provide value for --count'))

    with open(args.input) as file:
        cpcodes = ([int(line.strip()) for line in file])
    unique_cpcodes = list(set(cpcodes))
    logger.info(f'Import  total {len(cpcodes)} cpcodes')
    logger.info(f'Uniques total {len(unique_cpcodes)} cpcodes')
    cpcode_objects = [{'type': 'CP_CODE', 'id': cpcode} for cpcode in unique_cpcodes]

    payload = {}
    payload['name'] = args.eventname
    payload['start'] = f'{args.start}T00:00:00Z'
    payload['end'] = f'{args.end}T00:00:00Z'
    if args.frequency:
        payload['recurrence'] = {}
        payload['recurrence']['frequency'] = args.frequency
        payload['recurrence']['count'] = int(args.count)
    payload['objects'] = cpcode_objects
    if args.tags:
        payload['tags'] = args.tags

    account_switch_key, section, edgerc = args.account_switch_key, args.section, args.edgerc
    event = ec.EventCenterWrapper(account_switch_key=account_switch_key, section=section, edgerc=edgerc)
    resp = event.create_event(payload)
    if resp.ok:
        logger.debug(resp.json()[0].keys())
        event_name = resp.json()[0]['name']
        event_id = resp.json()[0]['id']
        logger.critical(f'Event: {event_name} [{event_id}] created successfully')
    else:
        logger.error(resp.status_code)
        logger.info('Payload:-')
        print_json(data=payload)
        logger.info('Response:-')
        print_json(data=resp.json())
