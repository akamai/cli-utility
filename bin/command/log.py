# Ghost Log Format Specification
# https://docs.akamai.com/esp/user/edgesuite/log-format.xml
from __future__ import annotations

import logging
import sys
import warnings
from urllib.parse import unquote

import dask.bag as db
import numpy as np
import pandas as pd
from akamai_utils import ghost_index as gh
from utils import files


logger = logging.getLogger(__name__)


def main(args, logger):
    if args.column and args.valuecontains is None:
        sys.exit(logger.error('At least one value is required for --value-contains'))

    line_count = files.get_line_count(args.input)
    logger.warning(f'Total {line_count:,} lines, please be patient') if line_count > 1000 else None

    filtered_column, valuecontains, sample = args.column, args.valuecontains, int(args.sample)
    sheet = {}
    alls = []

    if args.only == 'R':
        r_df, _ = process_line(args.input, record_type='r', column=filtered_column, search_keyword=valuecontains, sample=sample, logger=logger)
        line_count = len(r_df.index)
        if len(r_df.index) > 1:
            sheet['R'] = r_df
            alls.append(r_df)
        else:
            logger.warning('No r/R line found')
    elif args.only == 'F':
        f_df, _ = process_line(args.input, record_type='f', column=filtered_column, search_keyword=valuecontains, sample=sample, logger=logger)
        if len(f_df.index) > 0:
            sheet['F'] = f_df
            alls.append(f_df)
        else:
            logger.warning('No f/F line found')
    else:
        r_df, _ = process_line(args.input, record_type='r', column=filtered_column, search_keyword=valuecontains, sample=sample, logger=logger)
        line_count = len(r_df.index)
        if len(r_df.index) > 0:
            sheet['R'] = r_df
            alls.append(r_df)
        else:
            logger.warning('No r/R line found')

        f_df, _ = process_line(args.input, record_type='f', column=filtered_column, search_keyword=valuecontains, sample=sample, logger=logger)
        if len(f_df.index) > 0:
            sheet['F'] = f_df
            alls.append(f_df)
        else:
            logger.warning('No f/F line found')

    keys = sheet.keys()
    if len(keys) > 0:
        filepath = f'output/{args.output}' if args.output else 'output/ghost_log.xlsx'
        files.write_xlsx(filepath, sheet, freeze_column=3, freeze_row=5,
                         show_url=False,
                         show_index=True,
                         adjust_column_width=False)
        files.open_excel_application(filepath, True, pd.concat(alls))


def process_line(filename: str,
                 record_type: str,
                 column: str | None = None,
                 search_keyword: list | None = None,
                 sample: int | None = 0,
                 logger=None) -> tuple:

    if record_type == 'r':
        columns, rec_dict = gh.build_ghost_log_index('config/ghost_r.txt', logger)
    elif record_type == 'f':
        columns, rec_dict = gh.build_ghost_log_index('config/ghost_f.txt', logger)

    try:
        rec_dict[-1] = 'GMT'
        logger.warning(f'checking {record_type} line')
        bag = db.read_text(filename, compression='gzip')
    except:
        return pd.DataFrame(), pd.DataFrame()

    if sample > 0:
        bag_head = bag.take(sample)
        if record_type == 'r':
            processed_lines = [filter_r_line(line) for line in bag_head if filter_r_line(line) is not None]
        elif record_type == 'f':
            processed_lines = [filter_f_line(line) for line in bag_head if filter_f_line(line) is not None]
        else:
            pass
        ddf = db.from_sequence(processed_lines).to_dataframe(columns=columns)
    else:
        if record_type == 'r':
            processed_bag = bag.map(filter_r_line).filter(lambda x: x is not None)
        elif record_type == 'f':
            processed_bag = bag.map(filter_f_line).filter(lambda x: x is not None)
        else:
            pass
        ddf = processed_bag.to_dataframe(columns=columns)

    df = ddf.compute()
    df['starttime'] = pd.to_numeric(df['starttime'], errors='coerce')
    df['GMT'] = pd.to_datetime(df['starttime'], unit='s', utc=True)
    df['GMT'] = pd.to_datetime(df.GMT).dt.tz_localize(None)

    escape_columns = ['useragent', 'SNIservernamedata']
    for col in list(df.columns):
        if col in escape_columns:
            df[col] = df[col].apply(unquote)

    pd.options.mode.chained_assignment = None
    df = df.reset_index(drop=True)

    # find search filter in all columns, not column specific
    if column is None and search_keyword:
        mask = np.column_stack([df[col].astype(str).str.contains(f'{search_keyword}', na=False) for col in df])
        sdf = df.loc[mask.any(axis=1)]
        logger.warning(sdf.shape)

    # search based on column and value
    if column and search_keyword:
        logger.warning(f'Filtered {column} values: {search_keyword}')
        df[column] = df[column].astype(str)
        df = df[df[column].str.contains('|'.join(search_keyword))].copy()
        df = df.sort_values(by=['arl', 'starttime', 'ghostIP']).copy()
        df = df.reset_index(drop=True)
        line_count = len(df.index)
        if line_count > 1:
            logger.warning(f'Total {line_count:,} lines after filter')

    df_filtered = build_header_line(df, rec_dict, record_type, logger=logger)
    return df_filtered, None


def build_header_line(df: pd.Dataframe, record: dict, record_type: str, logger=None) -> pd.DataFrame:
    show_columns = list(df.columns)
    show_columns = show_columns[-1:] + show_columns[:-1]  # rearrange columns

    show_index = [str(k) for k, v in record.items() if v in show_columns]
    show_index = show_index[-1:] + show_index[:-1]
    logger.debug(f'{len(show_index)} {show_index}')
    df = df[show_columns]

    df.loc[-1] = show_index
    df.index = df.index + 1
    df = df.sort_index()

    df.loc[-1] = df.loc[0].apply(lambda x: f'{record_type}{x}' if int(x) > 0 else x)
    df.index = df.index + 1
    df = df.sort_index()

    df.loc[-1] = df.loc[1].apply(lambda x: gh.log_format_url(record=record_type, col_number=x) if int(x) > 0 else x)
    df.index = df.index + 1
    df = df.sort_index()

    url = 'https://docs.akamai.com/esp/user/edgesuite/log-format.xml#'
    df.loc[-1] = df.loc[1].apply(lambda x: files.create_hyperlink_to_external_link(url, x) if x not in ['0', '-1'] else x)
    df.index = df.index + 1
    df = df.sort_index()

    df = df.drop(labels=[1, 2, 3], axis=0)
    df = df.sort_index()

    # remove columns if all of them are '-'
    mask = (df.iloc[1:] == '-').all()
    columns_to_drop = df.columns[mask].tolist()
    logger.critical('Dropped columns')
    logger.info(columns_to_drop)
    mask = (df.iloc[1:] != '-').any()
    df_filtered = df.loc[:, mask]
    return df_filtered


def filter_r_line(line):
    values = line.strip().split(' ')
    if values[1] in ['r', 'R']:
        return tuple(values[:-2])


def filter_f_line(line):
    values = line.strip().split(' ')
    if values[1] in ['f', 'F']:
        return tuple(values)


def foo():
    pass
    # TODO add tab to store unique values for some fields
    '''
    urls = sorted(r_df['arl'].unique().tolist())
    print(*urls, sep="\n")
    logger.info(len(urls))

    df= pd.DataFrame(urls)
    files.write_xlsx(f'output/urls.xlsx', {'sheet1': df})
    '''
