from __future__ import annotations

import csv
import ipaddress
import sys

import numpy as np
import pandas as pd
import pydig
from akamai_api.gtm import GtmWrapper
from pandarallel import pandarallel
from rich import print_json
from utils import files


def get_nickname(x):
    try:
        return x[1]['nickname']
    except (KeyError, IndexError):
        return 0  #


def validate_ip(value):
    if pd.isna(value) or value == '0':
        return False
    try:
        new_value = ipaddress.ip_address(value)
        if value == new_value:
            return True
    except ValueError:
        x = pydig.query(value, 'A')
        new_value = ''
        try:
            if isinstance(x, list) and len(x) == 0:
                return False
            elif isinstance(x, list) and len(x) > 0:
                new_value = ipaddress.ip_address(x[0])
                if x[0] == new_value:
                    return True
            else:
                new_value = ipaddress.ip_address(x)
                if x == new_value:
                    return True
        except ValueError:
            if x[0] == new_value:
                return True
        return False


def audit(args, account_folder, logger):

    filepath = f'{account_folder}/{args.output}' if args.output else f'{account_folder}/gtm.xlsx'
    gtm = GtmWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)

    status, resp = gtm.list_domains()
    if status != 200:
        print_json(data=resp)
        sys.exit()

    master_df = pd.DataFrame()
    combine_df = []
    sheet = {}

    for i, x in enumerate(resp['items']):
        domain = resp['items'][i]['name']
        _, resp_domain = gtm.get_domain(domain)
        try:
            property = resp_domain['properties']
        except:
            logger.error(f'{i} {domain}')
            print_json(data=resp_domain)

        for ind, p in enumerate(property):
            prop_dict = property[ind]['trafficTargets']
            property_name = property[ind]['name']

            df = pd.DataFrame(prop_dict)
            df['Domain'] = domain
            df['Properties'] = property_name
            # df = df[df['enabled']].copy()
            # df = df[df['weight'] > 0].copy()
            try:
                df['Type'] = property[ind]['type']
            except:
                logger.error(f'{i} {domain} {property_name}')
                print_json(data=property)
            df['servers'] = df['servers'].str[0]
            combine_df.append(df)

    master_df = pd.concat(combine_df)
    cols = ['Domain', 'Properties', 'servers', 'handoutCName']

    master_df = master_df[master_df['enabled']].copy()
    master_df = master_df[master_df['weight'] > 0].copy()
    master_df['servers'] = master_df['servers'].fillna(master_df['handoutCName'])

    pandarallel.initialize(progress_bar=False, nb_workers=5, verbose=0)
    master_df['datacenter'] = master_df.parallel_apply(lambda row: gtm.get_datacenter(row['Domain'], row['datacenterId']), axis=1)
    master_df['datacenter'] = master_df['datacenter'].parallel_apply(get_nickname)
    master_df['valid_ip'] = master_df['servers'].parallel_apply(validate_ip)
    master_df['gtm'] = master_df['Properties'] + '.' + master_df['Domain']

    sort_col_df = master_df[['gtm', 'Domain', 'Properties', 'Type', 'datacenter', 'weight', 'servers']]
    master_df = sort_col_df.sort_values(by=['Domain', 'Properties', 'weight'],
                                        ascending=[True, True, False],
                                        )

    # cumulative counts of group by object
    cc = master_df.groupby(['gtm', 'Domain', 'Properties', 'Type']).cumcount() + 1
    flat_df = master_df.set_index(['gtm', 'Domain', 'Properties', 'Type', cc]
                                  ).unstack().sort_index(axis='columns', level=1)
    flat_df.columns = ['_'.join(map(str, i)) for i in flat_df.columns]
    flat_df.reset_index(inplace=True)

    sheet['flat_table'] = flat_df
    sheet['raw_data'] = master_df

    group_df = master_df[['gtm', 'datacenter', 'servers']]
    grouped = group_df.groupby('gtm').agg(lambda x: x.tolist()).reset_index()
    sheet['summary'] = grouped
    if sheet:
        files.write_xlsx(filepath, sheet, freeze_column=3)
        files.open_excel_application(filepath, not args.no_show, flat_df)


def remove_gtm_property(args, logger):
    gtm = GtmWrapper(account_switch_key=args.account_switch_key, section=args.section, edgerc=args.edgerc, logger=logger)
    print()
    with open(args.input, newline='') as csvfile:
        line = csv.DictReader(csvfile, delimiter=',')
        count = 0
        for row in line:
            try:
                domain = row['domain']
            except KeyError:
                logger.error('please correct header to domain')
                count += 1

            try:
                property = row['property']
            except KeyError:
                logger.error('please correct header to property')
                count += 1

            if count > 1:
                sys.exit()
            else:
                status, resp = gtm.get_property(domain, property)
                msg = f'domain: {domain:<40} property: {property:<20}'

                if status == 200:
                    logger.info(f'{msg}removed success')
                elif status == 404:
                    logger.error(f'{msg}invalid input')
                else:
                    print_json(data=resp)
