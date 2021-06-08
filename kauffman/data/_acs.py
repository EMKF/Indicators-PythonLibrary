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


def acs(series_lst):
    """
        https://api.census.gov/data/2019/acs/acs1/variables.html

        'B24081_001E': 'total',
        'B24081_002E': 'private',
        'B24081_003E': 'private_employee',
        'B24081_004E': 'private_self_employed',
        'B24081_005E': 'non_profit',
        'B24081_006E': 'local_government',
        'B24081_007E': 'state_government',
        'B24081_008E': 'federal_government',
        'B24081_009E': 'self_employed_not_inc',
        'B24092_001E': 'total_m',
        'B24092_002E': 'private_m',
        'B24092_003E': 'private_employee_m',
        'B24092_004E': 'private_self_employed_m',
        'B24092_005E': 'non_profit_m',
        'B24092_006E': 'local_government_m',
        'B24092_007E': 'state_government_m',
        'B24092_008E': 'federal_government_m',
        'B24092_009E': 'self_employed_not_inc_m',
        'B24092_010E': 'total_f',
        'B24092_011E': 'private_f',
        'B24092_012E': 'private_employee_f',
        'B24092_013E': 'private_self_employed_f',
        'B24092_014E': 'non_profit_f',
        'B24092_015E': 'local_government_f',
        'B24092_016E': 'state_government_f',
        'B24092_017E': 'federal_government_f',
        'B24092_018E': 'self_employed_not_inc_f'
    """
    return _acs_data_create(series_lst)
