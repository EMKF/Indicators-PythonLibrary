import os
import sys
import json
import joblib
import requests
import pandas as pd
import kauffman_data.constants as c
import kauffman_data.public_data_helpers

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


def _annualizer(df, annualize):
    if annualize:
        return df.\
            query('period == "M13"').\
            drop('period', 1).\
            rename(columns={'year': 'time'})
    return df.\
        query('period != "M13"'). \
        assign(time=lambda x: x['year'].astype(str) + '-' + x['period'].str[1:]). \
        drop(['year', 'period'], 1)


def get_data(series_lst=None, obs_level='us', start_year=None, end_year=None, seasonally_adj=True, annualize=False):
    """
    The self-employment data for sub-US levels must come from IRS SOI data: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-2017-zip-code-data-soi.
    Still holding out for https://download.bls.gov/pub/time.series/la/    local area unemployment statistics.
        * looks like no. ACS has some, but not current

    M13 indicates an annual average.

    series_lst: int
        LNU11000000 --- Civilian labor force; not seasonally adjusted.
        LNU02048984 --- Employment level of incorporated, self-employed workers; not seasonally adjusted; number
            reported in thousands of dollars.
        LNU02027714 --- Employment level of unincorporated, self-employed workers; not seasonally adjusted; number
            reported in thousands of dollars.

    annualize: bool
        If True, then returns the annual average.
    """

    series_dict = {
        'inc_self_employment': 'LNU02048984',
        'uninc_self_employment': 'LNU02027714',
        'civilian_labor_force': 'LNS11000000'
    }
    # 'civilian_labor_force': 'LNS11000000'
    if not series_lst:
        series_lst = list(series_dict.keys())

    if not start_year:
        start_year = 0
    if not end_year:
        end_year = pd.datetime.now().year

    # pd.read_csv('https://download.bls.gov/pub/time.series/ln/ln.data.1.AllData', sep='\t').to_csv(c.filenamer('../scratch/raw.csv'), index=False)
    df = pd.read_csv(c.filenamer('../scratch/raw.csv')).\
            rename(columns=lambda x: x.strip()).\
            assign(series_id=lambda x: x['series_id'].str.strip(), value=lambda x: x['value'] * 1000).\
            drop('footnote_codes', 1)  #.
            # query('series_id in {series_lst}'.format(series_lst=list(series_dict.values())))

    for ind, series in enumerate(series_lst):
        df_temp = df. \
            query('series_id == "{series_id}"'.format(series_id=series_dict[series])). \
            drop('series_id', 1). \
            rename(columns={'value': series})
        print(df_temp.head())

        if ind == 0:
            df_out = df_temp
        else:
            df_out = df_out.merge(df_temp, how='outer', on=['year', 'period'])

    return df_out.\
            query('year >= {start_year}'.format(start_year=start_year)).\
            query('year <= {end_year}'.format(end_year=end_year)).\
            pipe(_annualizer, annualize).\
            sort_values('time').\
            reset_index(drop=True) \
            [['time', 'inc_self_employment', 'uninc_self_employment']]
    # return df.assign(series_id=lambda x: x['series_id'].str.strip()). \
    #     query('series_id == "LNU02048984"'). \
    #     drop(['footnote_codes', 'series_id'], 1). \
    #     rename(columns={'value': 'inc_self_employment'}). \
    #     assign(uninc_self_employment=lambda x: x['inc_self_employment'] * 1000).\
    #     merge(
    #         df.\
    #             assign(series_id=lambda x: x['series_id'].str.strip()).\
    #             query('series_id == "LNU02027714"').\
    #             drop(['footnote_codes', 'series_id'], 1).\
    #             rename(columns={'value': 'uninc_self_employment'}).\
    #             assign(uninc_self_employment=lambda x: x['uninc_self_employment'] * 1000),
    #         how='outer',
    #         on=['year', 'period']
    #     ).\
    #     query('year >= {start_year}'.format(start_year=start_year)).\
    #     query('year <= {end_year}'.format(end_year=end_year)).\
    #     pipe(_annualizer, annualize).\
    #     sort_values('time').\
    #     reset_index(drop=True) \
    #     [['time', 'inc_self_employment', 'uninc_self_employment']]

if __name__ == '__main__':
    df = get_data()  #.to_csv(c.filenamer('../scratch/se_all_time.csv'), index=False)
    print(df.info())
    sys.exit()
    #
    df = pd.read_csv(c.filenamer('../scratch/se_all_time.csv'))
    print(df.info())
    print(df.head())

    print(get_data(annualize=True).head(20))


# todo: multiply value by 1000
# todo: rate; get total firms or employees or something.
# todo: farm/nonfarm?