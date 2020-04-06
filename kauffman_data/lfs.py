import os
import sys
import json
import joblib
import requests
import pandas as pd
import kauffman_data.constants as c

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def get_data_api(series_lst='self_employed', obs_level='us', start_year=None, end_year=None, seasonally_adj=True):
    """'Year range has been reduced to the system-allowed limit of 20 years.' Cuss.
    """
    headers = {'Content-type': 'application/json'}
    data = {
            "seriesid": ['LNU02048984', 'LNU02027714'],
            "registrationkey": os.getenv('BLS_KEY'),
        }
    if start_year:
        data["startyear"] = "{start_year}".format(start_year=start_year)
        if end_year:
            data["endyear"] = "{end_year}".format(end_year=end_year)
        else:
            data["endyear"] = pd.datetime.now().year

    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=json.dumps(data), headers=headers)
    print(p.status_code)
    print(p.json())
    for ind, series in enumerate(p.json()['Results']['series']):
        df_temp = pd.DataFrame.from_dict(series['data']). \
            rename(columns={'value': series['seriesID'], 'latest': 'latest_{}'.format(series['seriesID'])}). \
            drop('footnotes', 1)
        print(df_temp.head())
        print(df_temp.info())

        if ind == 0:
            df = df_temp
        else:
            df = df.merge(df_temp, how='right', on=['year', 'period', 'periodName'])
    return df


def get_data(series_lst='self_employed', obs_level='us', start_year=None, end_year=None, seasonally_adj=True):
    if not start_year:
        start_year = 0
    if not end_year:
        end_year = pd.datetime.now().year

    df = pd.read_csv('https://download.bls.gov/pub/time.series/ln/ln.data.1.AllData', sep='\t')
    df.columns = map(lambda x: x.strip(), df.columns.tolist())
    return df.assign(series_id=lambda x: x['series_id'].str.strip()).\
        query('series_id in ["LNU02048984", "LNU02027714"]'). \
        query('year >= {start_year}'.format(start_year=start_year)).\
        query('year <= {end_year}'.format(end_year=end_year)).\
        drop('footnote_codes', 1).\
        reset_index(drop=True)


if __name__ == '__main__':
    get_data().to_csv(c.filenamer('../scratch/se_all_time.csv'), index=False)
    #
    # df = pd.read_csv(c.filenamer('../scratch/se_all_time.csv'))
    # print(df.info())
    # print(df.head())