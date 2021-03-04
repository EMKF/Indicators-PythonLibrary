import sys
import datetime
import pandas as pd
from kauffman import constants as c

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _url(region, series, seasonally_adj):
    if 'DUR' in series:
        adjusted = 'no'
    else:
        if seasonally_adj:
            adjusted = 'yes'
        else:
            adjusted = 'no'
    return 'https://www.census.gov/econ/currentdata/export/csv?programCode=BFS&timeSlotType=12&startYear=2004&endYear=2021&categoryCode=TOTAL&' + \
          f'dataTypeCode={series}&' + \
          f'geoLevelCode={region}&' + \
          f'adjusted={adjusted}&' + \
          'errorData=no&internal=false'


def _year_create_shift(x):
    if x['time'].month <= 3:
        return x['time'].year
    return x['time'].year + 1


def _time_annualize(df, march_shift):
    if march_shift:
        return df.assign(time=lambda x: x.apply(_year_create_shift, axis=1))
    return df.assign(time=lambda x: x['time'].dt.year)


def _DUR_numerator(df):
    if 'BF_DUR4Q' in df.columns:
        df = df.assign(DUR4_numerator=lambda x: x['BF_DUR4Q'] * x['BF_BF4Q'])
    if 'BF_DUR8Q' in df.columns:
        df = df.assign(DUR8_numerator=lambda x: x['BF_DUR8Q'] * x['BF_BF8Q'])
    return df


def _BF_DURQ(df):
    if 'BF_DUR4Q' in df.columns:
        df = df.\
            assign(BF_DUR4Q=lambda x: x['DUR4_numerator'] / x['BF_BF4Q']).\
            drop('DUR4_numerator', 1)
    if 'BF_DUR8Q' in df.columns:
        df = df.\
            assign(BF_DUR8Q=lambda x: x['DUR8_numerator'] / x['BF_BF8Q']). \
            drop('DUR8_numerator', 1)
    return df


def _annualize(df, annualize, bf_helper_lst, march_shift):
    if annualize:
        return df.\
            pipe(_time_annualize, march_shift). \
            pipe(_DUR_numerator). \
            groupby(['fips', 'region', 'time']).sum(min_count=12).\
            pipe(_BF_DURQ).\
            reset_index(drop=False) \
            [[col for col in df.columns if col not in bf_helper_lst]]
    return df


def _bfs_data_create(region, series_lst, seasonally_adj, annualize, march_shift):
    print(f'Fetching BFS for {region}')
    if march_shift: annualize = True

    bf_helper_lst = []
    if annualize:
        if ('BF_DUR4Q' in series_lst) and ('BF_BF4Q' not in series_lst): bf_helper_lst.append('BF_BF4Q')
        if ('BF_DUR8Q' in series_lst) and ('BF_BF8Q' not in series_lst): bf_helper_lst.append('BF_BF8Q')

    df = pd.DataFrame(columns=['Period'])
    for series in series_lst + bf_helper_lst:
        df = pd.read_csv(_url(region, series, seasonally_adj), skiprows=7).\
            rename(columns={'Value': series}).\
            merge(df, how='outer', on='Period')

    return df. \
        assign(
            time=pd.to_datetime(df['Period'], format='%b-%Y'),
            region=c.abb_name_dic[region],
            fips=lambda x: c.abb_fips_dic[region],
        ). \
        drop('Period', 1). \
        pipe(_annualize, annualize, bf_helper_lst, march_shift) \
        [['fips', 'region', 'time'] + series_lst]
