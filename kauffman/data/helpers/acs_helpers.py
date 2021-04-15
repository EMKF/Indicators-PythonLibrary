import sys
import joblib
import requests
import pandas as pd
from kauffman import constants as c


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _fetch_data(year, var_set):
    var_lst = ','.join(var_set)
    url = 'https://api.census.gov/data/{year}/acs/acs1?get={var_lst}&for=us:*'.format(var_lst=var_lst, year=year)
    r = requests.get(url)
    return pd.DataFrame(r.json())


# todo: I think this can be replaced with a parameter in pd.DataFrame()
def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:, :]


def _acs_data_create(series_lst):
    return pd.concat(
        [
            _fetch_data(year, series_lst). \
                pipe(_make_header). \
                rename(columns=c.acs_outcomes). \
                drop('us', 1). \
                reset_index(drop=True). \
                assign(year=year)
            for year in range(2005, 2019 + 1)
        ],
        axis=0
    )
