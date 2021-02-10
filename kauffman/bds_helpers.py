import sys
import requests
import pandas as pd
import kauffman.constants as c


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:]


def _bds_data_create(variables, region):
    url = f'https://api.census.gov/data/timeseries/bds?get={",".join(variables)}&for={region}:*&YEAR=*'
    return pd.DataFrame(requests.get(url).json()).\
        pipe(_make_header).\
        rename(columns={'county': 'fips', 'state': 'fips', 'us': 'fips', 'YEAR': 'time'}).\
        assign(
            fips=lambda x: '00' if region == 'us' else x['fips'],
            region=lambda x: x['fips'].map(c.fips_abb_dic)
        ).\
        astype({var: 'int' for var in variables}).\
        sort_values(['fips', 'time']).\
        reset_index(drop=True)

# todo: line 32 needs some attention. It breaks because I need the county info in a dictionary.
# todo: for this problem, instead of the abbreviation, put the region name.