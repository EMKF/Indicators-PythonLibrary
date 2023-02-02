import os
from kauffman import constants as c
from kauffman.tools import api_tools as api


def _acs_fetch_data(year, var_set, geo_level, state_list, key, s):
    var_list = ','.join(var_set)
    base_url = f'https://api.census.gov/data/{year}/acs/acs1?get={var_list}'
    state_section = ','.join(state_list)
    
    in_state = True if geo_level == 'county' else False
    if geo_level == 'state':
        fips = state_section
    elif geo_level == 'msa':
        msas = [m for state in state_list for m in c.STATE_TO_MSA_FIPS[state]]
        fips = ",".join(msas)
    else:
        fips = '*'
    fips_section = '&for=' \
        + api._fips_section(geo_level, fips, state_section, in_state)

    key_section = f'&key={key}' if key else ''
    url = base_url + fips_section + key_section
    return api.fetch_from_url(url, s).assign(time=year)


def acs(
    series_list='all', geo_level='us', state_list='all',
    key=os.getenv("CENSUS_KEY"), n_threads=1
):
    """
    Fetches and cleans American Community Survey (ACS) data from the Census's
    API (https://api.census.gov/data.html, see the acs > acs1 datasets).

    Parameters
    ----------
    series_list: list or 'all', default 'all'
        The list of variables to fetch. The list of options can be found at:
        https://api.census.gov/data/2019/acs/acs1/variables.html. If 'all', the 
        following variables will be included:
        * 'B24081_001E': 'total',
        * 'B24081_002E': 'private',
        * 'B24081_003E': 'private_employee',
        * 'B24081_004E': 'private_self_employed',
        * 'B24081_005E': 'non_profit',
        * 'B24081_006E': 'local_government',
        * 'B24081_007E': 'state_government',
        * 'B24081_008E': 'federal_government',
        * 'B24081_009E': 'self_employed_not_inc',
        * 'B24092_001E': 'total_m',
        * 'B24092_002E': 'private_m',
        * 'B24092_003E': 'private_employee_m',
        * 'B24092_004E': 'private_self_employed_m',
        * 'B24092_005E': 'non_profit_m',
        * 'B24092_006E': 'local_government_m',
        * 'B24092_007E': 'state_government_m',
        * 'B24092_008E': 'federal_government_m',
        * 'B24092_009E': 'self_employed_not_inc_m',
        * 'B24092_010E': 'total_f',
        * 'B24092_011E': 'private_f',
        * 'B24092_012E': 'private_employee_f',
        * 'B24092_013E': 'private_self_employed_f',
        * 'B24092_014E': 'non_profit_f',
        * 'B24092_015E': 'local_government_f',
        * 'B24092_016E': 'state_government_f',
        * 'B24092_017E': 'federal_government_f',
        * 'B24092_018E': 'self_employed_not_inc_f'
    geo_level: {'us', 'state', 'msa', 'county'}, default 'us'
        The geographical level of the data.
    state_list: list or 'all', default 'all'
        The list of states to include in the data, identified by postal code 
        abbreviation. (Ex: 'AK', 'UT', etc.) Not available for geo_level = 'us'.
    key: str, default os.getenv("CENSUS_KEY"), optional
        Census API key. See README for instructions on how to get one, if 
        desired. Otherwise, user can pass key=None, which will work until the
        Census's data limit is exceeded.
    n_threads: int, default 1
        Number of threads to use for multithreading when fetching the data.
        n_threads=1 corresponds to no parallelization, and more threads 
        corresponds to more urls being pulled at a time. The optimal number of
        threads depends on the user's machine and the amount of data being 
        pulled.
    """
    # Handle series_list
    if series_list == 'all':
        series_list = [k for k,v in c.ACS_CODE_TO_VAR.items()]        

    # Handle state_list
    if state_list == 'all' or geo_level == 'msa':
        state_list = [c.STATE_ABB_TO_FIPS[s] for s in c.STATES]
    else:
        state_list = [c.STATE_ABB_TO_FIPS[s] for s in state_list]

    # Warn users if they didn't provide a key
    if key == None:
        print('WARNING: You did not provide a key. Too many requests will ' \
            'result in an error.')

    years = list(range(2005, 2019 + 1))
    index = ['time', 'fips', 'region', 'geo_level']

    return api.run_in_parallel(
            data_fetch_fn = _acs_fetch_data,
            groups = years,
            constant_inputs = [series_list, geo_level, state_list, key],
            n_threads = n_threads
        ) \
            .pipe(api._create_fips, geo_level) \
            .assign(geo_level=geo_level) \
            [index + series_list] \
            .rename(columns=c.ACS_CODE_TO_VAR) \
            .sort_values(index) \
            .reset_index(drop=True)