# Ghost Log Format Specification
# https://docs.akamai.com/esp/user/edgesuite/log-format.xml
from __future__ import annotations

import codecs
import platform
import subprocess
import sys
import warnings

import numpy as np
import pandas as pd
from akamai_utils import ghost_index as gh
from tabulate import tabulate
from utils import _logging as lg
from utils import files


logger = lg.setup_logger()


def main(args):
    warnings.filterwarnings('ignore')

    if args.column and args.value_contains is None:
        sys.exit(logger.error('At least one value is required for --value-contains'))

    line_count = files.get_line_count(args.input)
    logger.warning(f'Total {line_count:,} lines, please be patient') if line_count > 1000 else None

    sheet = {}

    if args.only == 'R':
        r_df = r_line(args.input, args.column, args.value_contains)
        if not r_df.empty:
            if r_df.shape[0] > 1:
                sheet['R'] = r_df
            else:
                logger.warning('No r/R line found')

    if args.only == 'F':
        f_df, _ = f_line(args.input, args.column, args.value_contains)
        if not f_df.empty:
            if f_df.shape[0] > 1:
                sheet['F'] = f_df
            else:
                logger.warning('No f/F line found')

    keys = sheet.keys()
    if len(keys) > 0 and args.output is not None:
        filepath = f'output/{args.output}'
        if sheet:
            files.write_xlsx(f'output/{args.output}', sheet, freeze_column=3, freeze_row=5,
                            show_url=False,
                            show_index=True,
                            adjust_column_width=False)
            if platform.system() != 'Darwin':
                logger.info('--show argument is supported only on Mac OS')
            else:
                subprocess.check_call(['open', '-a', 'Microsoft Excel', filepath])
                logger.info('https://docs.akamai.com/esp/user/edgesuite/log-format.xml')


def r_line(filename, column: str | None = None, search_keyword: list | None = None) -> pd.DataFrame:
    """
    Convert r/R lines from QGREP log to excel
    """

    try:
        r_columns, r_dict = gh.build_ghost_log_index('bin/config/ghost_r.txt')
    except:
        r_columns, r_dict = gh.build_ghost_log_index('config/ghost_r.txt')

    r_dict[-1] = 'GMT'
    logger.warning('checking r line')

    # https://pandas.pydata.org/docs/reference/api/pandas.errors.ParserWarning.html
    df = pd.read_csv(filename, header=None, sep=' ', names=r_columns,
                     index_col=False,
                     engine='python',
                     )

    df['GMT'] = pd.to_datetime(df['starttime'], unit='s', utc=True)
    df['GMT'] = pd.to_datetime(df.GMT).dt.tz_localize(None)
    # Set pd.options.mode.chained_assignment to 'warn'
    pd.options.mode.chained_assignment = 'warn'
    logger.debug(df.dtypes)
    # Assuming df is your DataFrame and 'useragent' is the column
    df['useragent'] = df['useragent'].astype(str)

    pd.options.mode.chained_assignment = None
    df = df.loc[df['record'] == 'r'].copy()

    # find search filter in all columns, not column specific
    if column is None and search_keyword:
        mask = np.column_stack([df[col].astype(str).str.contains(f'{search_keyword[0]}', na=False) for col in df])
        df = df.loc[mask.any(axis=1)]
        logger.warning(df.shape)

    # seach based on column and value
    if column:
        if search_keyword is None:
            sys.exit(logger.error('At least one value is required for --value-contains'))
        else:
            logger.warning(f'Filtering {search_keyword}')
            df[column] = df[column].astype(str)
            df = df[df[column].str.contains('|'.join(search_keyword))].copy()

    # drop columns that have the same value
    nunique = df.nunique()
    cols_to_drop = nunique[nunique == 1].index
    # df = df.drop(cols_to_drop, axis=1)

    # Only check Object Status 1
    # df = df[df['Object_Status_1'].str.contains('uZ') ]
    # df = df[df['Object_Status_17].str.contains('uZ') == False ]

    df = df.sort_values(by=['arl', 'starttime', 'ghostIP']).copy()
    df = df.reset_index(drop=True)

    show_columns = list(df.columns)
    show_columns = show_columns[-1:] + show_columns[:-1]

    show_index = [str(k) for k, v in r_dict.items() if v in show_columns]
    show_index = show_index[-1:] + show_index[:-1]
    logger.debug(f'{len(show_index)} {show_index}')
    df = df[show_columns]

    temp_columns = ['GMT', 'ghostIP', 'useragent',  'clientIP']
    tdf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{tdf}')

    df.loc[-1] = show_index
    df.index = df.index + 1
    df = df.sort_index()
    adf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{adf}')

    df.loc[-1] = df.loc[0].apply(lambda x: f'r{x}' if int(x) > 0 else x)
    df.index = df.index + 1
    df = df.sort_index()
    bdf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{bdf}')

    df.loc[-1] = df.loc[1].apply(lambda x: gh.log_format_url(record='r', col_number=x) if int(x) > 0 else x)
    df.index = df.index + 1
    df = df.sort_index()
    cdf = df.head(10)[temp_columns].copy()
    logger.debug(f'\n{cdf}')

    url = 'http://lp.engr.akamai.com/log-format.xml#'
    df.loc[-1] = df.loc[1].apply(lambda x: files.make_xlsx_hyperlink_to_external_link(url, x) if x not in ['0', '-1'] else x)

    df.index = df.index + 1
    df = df.sort_index()
    ddf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{ddf}')

    df = df.drop(labels=[1, 2, 3], axis=0)
    df = df.sort_index()
    df = df.reset_index(drop=True)
    edf = df.head(10)[temp_columns].copy()
    # logger.info(f'Sample first 10 rows')
    # print(tabulate(edf[temp_columns], headers=temp_columns, tablefmt='github', numalign='center'))
    logger.warning(df.shape)

    return df


def f_line(filename: str, search_keyword: list | None = None) -> tuple:
    """
    Convert f/F lines from QGREP log to excel
    """

    try:
        f_columns, f_dict = gh.build_ghost_log_index('bin/config/ghost_f.txt')
    except:
        f_columns, f_dict = gh.build_ghost_log_index('config/ghost_f.txt')

    try:
        logger.warning('checking f line')
        df = pd.read_csv(filename, header=0, sep=' ', names=f_columns,
                     # low_memory=False,
                     index_col=False,
                     engine='python',
                     # on_bad_lines='warn',
                     # nrows=20
                     )
    except:
        return pd.DataFrame(), pd.DataFrame()

    df['GMT'] = pd.to_datetime(df['starttime'], unit='s', utc=True)
    df['GMT'] = pd.to_datetime(df.GMT).dt.tz_localize(None)

    f_dict[-1] = 'GMT'

    pd.options.mode.chained_assignment = None
    df = df.loc[df['record'] == 'f'].copy()
    df = df.reset_index(drop=True)

    show_columns = list(df.columns)
    show_columns = show_columns[-1:] + show_columns[:-1]

    show_index = [str(k) for k, v in f_dict.items() if v in show_columns]
    show_index = show_index[-1:] + show_index[:-1]
    logger.debug(f'{len(show_index)} {show_index}')
    df = df[show_columns]

    temp_columns = ['GMT', 'ghostIP', 'starttime', 'ssloverhead']
    tdf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{tdf}')

    df.loc[-1] = show_index
    df.index = df.index + 1
    df = df.sort_index()
    adf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{adf}')

    df.loc[-1] = df.loc[0].apply(lambda x: f'f{x}' if int(x) > 0 else x)
    df.index = df.index + 1
    df = df.sort_index()
    bdf = df.head(5)[temp_columns].copy()
    logger.debug(f'\n{bdf}')

    df.loc[-1] = df.loc[1].apply(lambda x: gh.log_format_url(record='f', col_number=x) if int(x) > 0 else x)
    df.index = df.index + 1
    df = df.sort_index()
    cdf = df.head(10)[temp_columns].copy()
    logger.debug(f'\n{cdf}')

    url = 'http://lp.engr.akamai.com/log-format.xml#'
    df.loc[-1] = df.loc[1].apply(lambda x: files.make_xlsx_hyperlink_to_external_link(url, x) if x not in ['0', '-1'] else x)

    df.index = df.index + 1
    df = df.sort_index()
    ddf = df.head(10)[temp_columns].copy()
    logger.debug(f'\n{ddf}')

    df = df.drop(labels=[1, 2, 3], axis=0)
    df = df.sort_index()
    df = df.reset_index(drop=True)
    edf = df.head(10)[temp_columns].copy()

    logger.debug(f'\n{edf}')

    '''
    int_columns = ['ssloverhead', 'dnslookup', 'transfertime', 'bytesreceived', 'request_number']
    for column in int_columns:
        df[column] = df[column].apply(lambda x: x.replace('-', '0'))
        df[column] = df[column].astype('int64')
    '''

    str_columns = ['ghostIP', 'starttime', 'endtime', 'ssloverhead', 'dnslookup',
                   'turnaroundtime', 'transfertime', 'bytesreceived', 'forwardip', 'clientip', 'httpmethod',
                   'arl', 'objectstatus_1', 'request_number']
    stat_df = df[str_columns]
    return df, stat_df


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
