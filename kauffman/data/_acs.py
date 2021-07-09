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


def _build_region_section(region):
    if region in ['us', 'state', 'county']:
        return f'&for={region}:*'
    else:
        return '&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'


def _fetch_data(year, var_set, region):
    var_lst = ','.join(var_set)
    base_url = f'https://api.census.gov/data/{year}/acs/acs1?get={var_lst}'
    url = base_url + _build_region_section(region)
    r = requests.get(url)
    return pd.DataFrame(r.json())


# todo: I think this can be replaced with a parameter in pd.DataFrame()
def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:, :]


def _covar_create_fips_region(df, region):
    if region == 'state':
        df['fips'] = df['state'].astype(str)
    elif region == 'county':
        df['fips'] = df['state'].astype(str) + df['county'].astype(str)
        df = df.drop(columns='state')
    elif region == 'msa':
        df['fips'] = df['msa'].astype(str)
    else:
        df = df.assign(fips='00')
    return df.assign(region=lambda x: x['fips'].map(c.all_fips_to_name)).drop(columns=region)


def _acs_data_create(series_lst, region):
    return pd.concat(
        [
            _fetch_data(year, series_lst, region). \
                pipe(_make_header). \
                rename(columns=c.acs_code_to_var). \
                rename(columns={'metropolitan statistical area/micropolitan statistical area':'msa'}).\
                pipe(_covar_create_fips_region, region). \
                reset_index(drop=True). \
                assign(year=year)
            for year in range(2005, 2019 + 1)
        ],
        axis=0
    )


def acs(series_lst='all', obs_level='all'):
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
    # Handle series_lst
    if series_lst == 'all':
        series_lst = [k for k,v in c.acs_code_to_var.items()]

    # Create region_lst
    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level in ['us', 'state', 'msa', 'county']:
            region_lst = [obs_level]
        else:
            region_lst = ['us', 'state', 'msa', 'county']

    return pd.concat(
            [
                _acs_data_create(series_lst, region)
                for region in region_lst
            ],
            axis=0
        )
    

# TODO:
    # Figure out how to handle region column at the end (look at qwi)
    # Update docstring
