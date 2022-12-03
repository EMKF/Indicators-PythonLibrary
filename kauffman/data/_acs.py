import requests
import pandas as pd
from kauffman import constants as c
import os


def _build_region_section(region, state_lst):
    state_section = ','.join(state_lst)
    
    if region == 'us':
        return f'&for={region}:*'
    elif region == 'state':
        return f'&for={region}:{state_section}'
    elif region == 'county':
        return f'&for={region}:*&in=state:{state_section}'
    else:
        msa_list = []
        for state in state_lst:
            try:
                msa_list.extend(c.state_to_msa_fips[state])
            except:
                pass

        msa_section = ','.join(msa_list)

        return f'&for={c.api_msa_string}:{msa_section}'


def _fetch_data(year, var_set, region, state_lst, key):
    var_lst = ','.join(var_set)
    base_url = f'https://api.census.gov/data/{year}/acs/acs1?get={var_lst}&key={key}'
    url = base_url + _build_region_section(region, state_lst)
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
    return df \
        .assign(region=lambda x: x['fips'].map(c.all_fips_to_name)) \
        .drop(columns=region)


def index_to_front(df):
    index_cols = ['fips', 'region', 'year']
    return df[index_cols + [col for col in df.columns if col not in index_cols]]

def _acs_data_create(series_lst, region, state_lst, key):
    return pd.concat(
        [
            _fetch_data(year, series_lst, region, state_lst, key) \
                .pipe(_make_header) \
                .rename(columns=c.acs_code_to_var) \
                .rename(columns={c.api_msa_string:'msa'}) \
                .pipe(_covar_create_fips_region, region) \
                .assign(year=year) \
                .pipe(index_to_front)
            for year in range(2005, 2019 + 1)
        ],
        axis=0
    ).sort_values(['fips', 'region', 'year'])


def acs(series_lst='all', obs_level='all', state_lst = 'all', key=os.getenv("CENSUS_KEY")):
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

    obs_level: str or list
        'state': resident population of state from 2005 through 2019
        'msa': resident population of msa from 2005 through 2019
        'county': resident population of county from 2005 through 2019
        'us': resident population in the United States from 2005 through 2019
        'all': default, returns data on all of the above observation levels

    state_lst = str or list
        'all': default, includes all US states and D.C.
        otherwise: a state or list of states, identified using postal code 
            abbreviations

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

    if state_lst == 'all':
        state_lst = [c.state_abb_to_fips[s] for s in c.states]
    elif type(state_lst) == list:
        if obs_level != 'msa':
            state_lst = [c.state_abb_to_fips[s] for s in state_lst]
        else:
            state_lst = [c.state_abb_to_fips[s] for s in c.states]

    return pd.concat(
            [
                _acs_data_create(series_lst, region, state_lst, key)
                for region in region_lst
            ],
            axis=0
        ).reset_index(drop=True)