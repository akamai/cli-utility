from __future__ import annotations

import csv
import ipaddress
import sys

import pandas as pd
from akamai_api.gtm import GtmWrapper
from rich import print_json
from utils import files


def get_nickname(x):
    try:
        return x[1]['nickname']
    except (KeyError, IndexError):
        return 0  #


def validate_ip(x):
    if pd.isna(x) or x == '0':
        return None
    try:
        return ipaddress.ip_address(x)
    except ValueError:
        return None


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

            df['get_datacenter'] = df.apply(lambda row: gtm.get_datacenter(row['Domain'], row['datacenterId']), axis=1)
            df['get_datacenter'] = df['get_datacenter'].apply(get_nickname)

            try:
                df['Type'] = property[ind]['type']
            except:
                logger.error(f'{i} {domain} {property_name}')
                print_json(data=property)
            df['servers'] = df['servers'].str[0]
            combine_df.append(df)

    master_df = pd.concat(combine_df)
    master_df['valid_ip'] = master_df['servers'].apply(validate_ip)
    sort_col_df = master_df[['Domain', 'Properties', 'Type', 'enabled',
                             'weight', 'servers', 'name', 'handoutCName', 'valid_ip'
                             ]]
    master_df = sort_col_df.sort_values(by=['Domain', 'Properties', 'weight', 'enabled'],
                                        ascending=[True, True, False, False],
                                        )

    # cumulative counts of group by object
    cc = master_df.groupby(['Domain', 'Properties', 'Type']).cumcount() + 1
    flat_df = master_df.set_index(['Domain', 'Properties', 'Type', cc]
                                  ).unstack().sort_index(axis='columns', level=1)
    flat_df.columns = ['_'.join(map(str, i)) for i in flat_df.columns]
    flat_df.reset_index(inplace=True)

    sheet['flat_table'] = flat_df
    sheet['raw_data'] = master_df

    if sheet:
        files.write_xlsx(filepath, sheet)
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
