from __future__ import annotations

import pandas as pd
import swifter
from akamai_utils.papi import PapiWrapper
from pandarallel import pandarallel
from utils._logging import setup_logger
from utils.files import write_xlsx


logger = setup_logger()


def test(args):
    papi = PapiWrapper(account_switch_key=args.account_switch_key)
    _, parent_group_df = papi.get_parent_groups()
    level_2 = get_property_stat_child_group(args.account_switch_key, parent_group_df)


def lookup_details(df: pd.DataFrame, papi, collect_host: bool):
    df = df.reset_index()
    logger.warning('Collecting last updatedDate')
    df['updatedDate'] = df[['propertyId', 'latestVersion']].parallel_apply(lambda x: papi.get_properties_detail(*x), axis=1)

    logger.warning('Collecting rule format')
    pandarallel.initialize(progress_bar=True)
    df['ruleFormat'] = df[['propertyId', 'latestVersion']].parallel_apply(lambda x: papi.get_properties_ruletree_digest(*x), axis=1)
    pandarallel.initialize(progress_bar=False)
    if collect_host:
        property_ids = df['propertyId'].unique().tolist()
        property_dict = {}
        for i, property_id in enumerate(property_ids, 1):
            hostnames = papi.get_property_hostnames(property_id)
            logger.debug(f'{property_id} {type(property_id)} {len(hostnames)}')
            property_dict[property_id] = hostnames

        logger.warning('Assigning hostnames to dataframe and hostname_count')
        df['hostname'] = df['propertyId'].astype(str).map(property_dict)
        df['hostname_count'] = df['hostname'].str.len()
    else:
        df['hostname'] = ''
        df['hostname_count'] = 0
    return df.copy()


def get_property_stat_child_group(account_switch_key, parent_group_df):
    papi = PapiWrapper(account_switch_key)
    _, child_group_df = papi.get_groups()

    level_2 = child_group_df.merge(parent_group_df, left_on='parentGroupId', right_on='groupId')
    level_2 = level_2.drop(columns=['groupId_y', 'parentGroupId_y', 'contractIds_y'], axis=1)
    level_2 = level_2.rename({'groupName_y': 'parent_folder',
                              'groupName_x': 'groupName',
                              'groupId_x': 'groupId',
                              'parentGroupId_x': 'parentGroupId',
                              'contractIds_x': 'original_contractIds'}, axis=1)
    level_2 = level_2.sort_values(by=['order', 'groupName'])
    logger.warning('Cleaning contractIds')
    pandarallel.initialize(progress_bar=False)
    # level_2['contractIds'] = level_2[['parentGroupId', 'original_contractIds']].swifter.apply(lambda x: papi.get_correct_contract(*x), axis=1)
    level_2['contractIds'] = level_2[['parentGroupId', 'original_contractIds']].parallel_apply(lambda x: papi.get_correct_contract(*x), axis=1)

    logger.debug('Collecting property count')
    # level_2['size'] = level_2[['groupId', 'contractIds']].swifter.apply(lambda x: papi.get_properties_count_in_group(*x), axis=1)
    level_2['size'] = level_2[['groupId', 'contractIds']].parallel_apply(lambda x: papi.get_properties_count_in_group(*x), axis=1)
    level_2 = level_2.reset_index()
    columns = ['order', 'contractIds', 'parentGroupId', 'parent_folder', 'groupName', 'groupId', 'size']  # not display 'original_contractIds'
    level_2 = level_2[columns]
    return level_2.copy()


def main(args):

    sheets = {}
    papi = PapiWrapper(account_switch_key=args.account_switch_key)
    _, parent_group_df = papi.get_parent_groups()
    property_df, property_size = papi.get_properties_in_group()
    # property_df.drop(columns=['note'], axis=1, inplace=True)

    parent_group_df['size'] = parent_group_df['groupId'].swifter.apply(lambda x: property_size[x])
    sheets['parent_count'] = parent_group_df

    level_2 = get_property_stat_child_group(args.account_switch_key, parent_group_df)
    sheets['child_count'] = level_2

    # only have latest version
    latest_df = property_df[(property_df['stagingVersion'].isnull()) & (property_df['productionVersion'].isnull())]
    logger.debug(f'Only has LATEST version\n{latest_df}')
    latest = latest_df['propertyName'].values.tolist()

    # only have staging version
    staging_df = property_df[(property_df['productionVersion'].isnull()) & (property_df[['stagingVersion']].notnull().all(1))]
    logger.debug(f'Only has STAGING version\n{staging_df}')
    stg = staging_df['propertyName'].values.tolist()
    empty_versions = [*latest, *stg]

    # Getting information from Latest Version
    columns = ['accountId', 'contractId', 'groupId', 'assetId', 'propertyId', 'propertyName',
               'latestVersion', 'stagingVersion', 'productionVersion', 'updatedDate', 'ruleFormat', 'hostname_count', 'hostname']
    logger.critical('suspect_df')
    suspect_df = property_df[property_df['propertyName'].isin(empty_versions)].copy()

    suspect_df = lookup_details(suspect_df, papi, collect_host=False)
    sheets['ignored'] = suspect_df[columns]

    # Getting information from Production Version
    logger.critical('good_df')
    good_df = property_df[~property_df['propertyName'].isin(empty_versions)].copy()
    good_df = good_df.astype({'productionVersion': int})
    good_df = lookup_details(good_df, papi, collect_host=True)
    sheets['checked'] = good_df[columns]

    write_xlsx('output/nike.xlsx', sheets)
