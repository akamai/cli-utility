from __future__ import annotations

from datetime import datetime
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import polars as pl
from akamai_api.mpulse import mPulse
from rich import print_json


def generate_token(args, logger):
    mp = mPulse(args.apikey, logger)
    resp = mp.generate_token(args.tenant)
    if resp.ok:
        logger.warning(f"Authention Token: {resp.json()['token']}")
    else:
        logger.warning(f'{resp.status_code} {resp.json()}')


def generate_dates(args, logger):
    # Create a list of dates within the specified range
    begin_date = datetime.strptime(args.fromdate, '%Y%m%d')
    end_date = datetime.strptime(args.todate, '%Y%m%d')
    date_list = [begin_date + timedelta(days=x) for x in range((end_date - begin_date).days + 1)]

    for date in date_list:
        logger.debug(date.strftime('%Y-%m-%d'))
    return date_list


def list_available_account(args, logger):
    token, apikey, = args.token, args.apikey
    mp = mPulse(apikey, token, logger)

    resp = mp.list(args.type)
    logger.debug(f'{resp.status_code} {resp.url}')

    try:
        objects = resp.json()['objects']
        sorted_objects = [obj['name'] for obj in objects]
        logger.info(sorted_objects)
        # sorted_objects = sorted([obj['name'] for obj in objects], key=lambda x: x.lower())
    except (KeyError, ValueError) as e:
        sorted_objects = []
        logger.error(f'Error: {e}')

    logger.warning('mPulse App API Key')
    for i, obj in enumerate(sorted_objects, start=1):
        single_object = [object for object in objects if object['name'] == obj]
        apikey = [x for x in single_object[0]['attributes'] if x['name'] == 'apiKey']  # type=domain
        logger.info(f'{i:>5}. {obj:<40} {apikey[0]["value"]}')


def url(args, logger):
    token, apikey, = args.token, args.apikey
    mp = mPulse(apikey, token, logger)

    resp = mp.get_url()
    logger.info(f'{resp.status_code} {resp.url}')
    print_json(data=resp.json())


def pageload_overtime(args, logger):
    token, apikey, = args.token, args.apikey
    mp = mPulse(apikey, token, logger)

    resp = mp.pageload_overtime()
    logger.info(f'{resp.status_code} {resp.url}')
    print_json(data=resp.json())

    y = [x for x in resp.json()['values'] if x['id'] == 'PageLoad']
    print_json(data=resp.json())

    date_list = generate_dates(args, logger)
    df = pl.DataFrame({'date': date_list,
                       'PageLoad': y[0]['history'],
                       })
    logger.debug(f'\n{df}')

    begin_date = datetime.strptime(args.fromdate, '%Y%m%d')
    end_date = datetime.strptime(args.todate, '%Y%m%d')

    date_df = pl.date_range(begin_date, end_date, timedelta(days=1), eager=True)
    logger.debug(date_df)

    # for template in ["plotly", "plotly_white", "plotly_dark", "ggplot2", "seaborn", "simple_white", "none"]:
    # fig = px.line(df, x='date', y='PageLoad', render_mode="svg",  title='Page Load')
    # fig.show()

    fig = go.Figure(data=go.Scatter(x=df['date'],
                                    y=df['PageLoad'],
                                    marker_color='black',
                                    text='PageLoad'))
    fig.update_layout({'title': 'Page Load(s) in September, 2023',
                        'xaxis': {'title': 'Date'},
                        'yaxis': {'title': 'PageLoad'},
                        'showlegend': False})
    fig.update_xaxes(nticks=30)
    fig.show()
