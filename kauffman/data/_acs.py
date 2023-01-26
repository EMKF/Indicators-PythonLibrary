import os
import pandas as pd
from kauffman import constants as c
from kauffman.tools import api_tools as api


def _acs_fetch_data(year, var_set, region, state_lst, key, s):
    var_lst = ','.join(var_set)
    base_url = f'https://api.census.gov/data/{year}/acs/acs1?get={var_lst}'
    state_section = ','.join(state_lst)
    
    in_state = True if region == 'county' else False
    if region == 'state':
        fips = state_section
    elif region == 'msa':
        msas = [m for state in state_lst for m in c.STATE_TO_MSA_FIPS[state]]
        fips = ",".join(msas)
    else:
        fips = '*'
    fips_section = '&for=' \
        + api._fips_section(region, fips, state_section, in_state)

    key_section = f'&key={key}' if key else ''
    url = base_url + fips_section + key_section
    return api.fetch_from_url(url, s).assign(year=year)


def _acs_data_create(series_lst, region, state_lst, key, n_threads):
    years = list(range(2005, 2019 + 1))
    return api.run_in_parallel(
        _acs_fetch_data,
        years,
        [series_lst, region, state_lst, key], 
        n_threads
    ) \
        .pipe(api._create_fips, region) \
        [['fips', 'region', 'year'] + series_lst] \
        .rename(columns=c.ACS_CODE_TO_VAR) \
        .sort_values(['fips', 'region', 'year'])


def acs(
    series_lst='all', obs_level='all', state_lst='all',
    key=os.getenv("CENSUS_KEY"), n_threads=1
):
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
        series_lst = [k for k,v in c.ACS_CODE_TO_VAR.items()]

    # Create region_lst
    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level in ['us', 'state', 'msa', 'county']:
            region_lst = [obs_level]
        else:
            region_lst = ['us', 'state', 'msa', 'county']

    if state_lst == 'all':
        state_lst = [c.STATE_ABB_TO_FIPS[s] for s in c.STATES]
    elif type(state_lst) == list:
        if obs_level != 'msa':
            state_lst = [c.STATE_ABB_TO_FIPS[s] for s in state_lst]
        else:
            state_lst = [c.STATE_ABB_TO_FIPS[s] for s in c.STATES]

    # Warn users if they didn't provide a key
    if key == None:
        print('WARNING: You did not provide a key. Too many requests will ' \
            'result in an error.')

    return pd.concat(
            [
                _acs_data_create(series_lst, region, state_lst, key, n_threads)
                for region in region_lst
            ]
        ).reset_index(drop=True)