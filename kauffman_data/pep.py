import os
import sys
import joblib
import requests
import pandas as pd
from kauffman_data import constants as c

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _format_year(df):
    return df.\
        assign(year=lambda x: x['date'].str[:4]).\
        astype({'year': 'int'}). \
        drop('date', 1)


def _format_population(df):
    return df. \
        rename(columns={'value': 'population'}). \
        astype({'population': 'float'}).\
        assign(population=lambda x: x['population'] * 1000)


def _json_to_pandas_construct(state_dict):
    df = pd.DataFrame()
    for region, values in state_dict.items():
        df = df.append(
            pd.DataFrame(values) \
                [['date', 'value']]. \
                pipe(_format_year). \
                pipe(_format_population). \
                assign(region=region)
        )
    return df


def _data_transform(region, state_dict):
    if region == 'us':
        series_id = 'B230RC0A052NBEA'  # yearly data
        # series_id = 'POPTHM'  # monthly data
    else:
        series_id = region + 'POP'

    url = 'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={key}&file_type=json'.format(series_id=series_id, key='b6602eab475fc27e3ea2feaedd7ff81b')
    r = requests.get(url)
    state_dict[region] = r.json()['observations']


def _observations_filter(df, start_year, end_year):
    if start_year:
        df.query('year >= {start_year}'.format(start_year=start_year), inplace=True)
    if end_year:
        df.query('year <= {end_year}'.format(end_year=end_year), inplace=True)
    return df.reset_index(drop=True)


def get_data(obs_level, start_year=None, end_year=None):
    """
    Collects population data, similar to https://fred.stlouisfed.org/series/CAPOP, from FRED. Requires an api key...
    register here: https://research.stlouisfed.org/useraccount/apikey. For now, I'm just including my key until we
    figure out the best way to do this.

    (as of 2020.03.16)
    obs_level:
        'state': resident population of state from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019

    start_year: earliest start year is 1900

    end_year: latest end year is 2019
    """
    print('Collecting data for...')
    region_dict = {}
    if obs_level == 'state':
        for state in c.states:
            print(state)
            _data_transform(state, region_dict)
    elif obs_level == 'us':
        print(obs_level)
        _data_transform('us', region_dict)
    return _json_to_pandas_construct(region_dict). \
        pipe(_observations_filter, start_year, end_year)


if __name__ == '__main__':
    # df = get_data('state', 2011, 2018)
    df = get_data('us', 2011, 2018)
    print(df.head())
    print(df.info())
    print(df.shape)
