import requests
import pandas as pd
import kauffman.constants as c
import os
import numpy as np
from kauffman.tools import api_tools as api


def _bds_url(variables, geo_level, state_list, strata, key, year):
    flag_var = [f'{var}_F' for var in variables]
    var_string = ",".join(variables + strata + flag_var)
    state_string = ",".join(state_list)
    
    fips = state_string if geo_level == 'state' else '*'
    in_state = True if geo_level == 'county' else False
    fips_section = api._fips_section(geo_level, fips, state_string, in_state)

    naics_string = '&NAICS=00' if 'NAICS' not in strata else ''
    key_section = f'&key={key}' if key else ''
    
    return f'https://api.census.gov/data/timeseries/bds?get={var_string}' \
        f'&for={fips_section}&YEAR={year}{naics_string}{key_section}'


def _bds_fetch_data(year, variables, geo_level, state_list, strata, key, s):
    url = _bds_url(variables, geo_level, state_list, strata, key, year)
    return api.fetch_from_url(url, s)


def _mark_flagged(df, variables):
    df[variables] = df[variables] \
        .apply(
            lambda x: df[f'{x.name}_F'] \
                .where(df[f'{x.name}_F'].isin(['D', 'S', 'X']), x) \
                .replace(['D', 'S', 'X'], np.NaN)
        )
    return df  


def check_strata_valid(geo_level, strata):
    valid_crosses = c.BDS_VALID_CROSSES

    if not strata:
        valid = True
    elif geo_level in ['state', 'county', 'msa']:
        strata = set(strata + [geo_level.upper()])
        valid = strata in valid_crosses
    elif geo_level == 'all':
        valid = all(
            set(strata + [o.upper()]) in valid_crosses 
            for o in ['us', 'state', 'msa', 'county']
        )
    else:
        strata = set(strata)
        valid = strata in valid_crosses

    return valid


def bds(
    series_list='all', geo_level='us', state_list='all', strata=[], 
    get_flags=False, key=os.getenv('CENSUS_KEY'), n_threads=1
):
    """
    Fetches and cleans Business Dynamics Statistics (BDS) data from the Census's
    API (https://api.census.gov/data/timeseries/bds.html).

    Parameters
    ----------
    series_list: list or 'all', default 'all'
        List of variables to fetch. See:
            https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf 
            or 
            https://api.census.gov/data/timeseries/bds/variables.html

        If 'all', the following variables will be included:
        * DENOM: (DHS) denominator
        * EMP: Number of employees
        * ESTAB: Number of establishments
        * ESTABS_ENTRY: Number of establishments born during the last 12 months
        * ESTABS_ENTRY_RATE: Rate of establishments born during the last 12 
            months
        * ESTABS_EXIT: Number of establishments exited during the last 12 months
        * ESTABS_EXIT_RATE: Rate of establishments exited during the last 12 
            months
        * FIRM: Number of firms
        * FIRMDEATH_EMP: Number of employees associated with firm deaths during
            the last 12 months
        * FIRMDEATH_ESTABS: Number of establishments associated with firm deaths
            during the last 12 months
        * FIRMDEATH_FIRMS: Number of firms that exited during the last 12 months
        * JOB_CREATION: Number of jobs created from expanding and opening 
            establishments during the last 12 months
        * JOB_CREATION_BIRTHS: Number of jobs created from opening
            establishments during the last 12 months
        * JOB_CREATION_CONTINUERS: Number of jobs created from expanding 
            establishments during the last 12 months
        * JOB_CREATION_RATE: Rate of jobs created from expanding and opening 
            establishments during the last 12 months
        * JOB_CREATION_RATE_BIRTHS: Rate of jobs created from opening 
            establishments during the last 12 months
        * JOB_DESTRUCTION: Number of jobs lost from contracting and closing 
            establishments during the last 12 months
        * JOB_DESTRUCTION_CONTINUERS: Number of jobs lost from contracting 
            establishments during the last 12 months
        * JOB_DESTRUCTION_DEATHS: Number of jobs lost from closing 
            establishments during the last 12 months
        * JOB_DESTRUCTION_RATE: Rate of jobs lost from contracting and closing 
            establishments during the last 12 months
        * JOB_DESTRUCTION_RATE_DEATHS: Rate of jobs lost from closing 
            establishments during the last 12 months
        * NET_JOB_CREATION: Number of net jobs created from expanding/
            contracting and opening/closing establishments during the last 12 
            months
        * NET_JOB_CREATION_RATE: Rate of net jobs created from expanding/
            contracting and opening/closing establishments during the last 12 
            months
        * REALLOCATION_RATE: Rate of reallocation during the last 12 months

    geo_level: {'us', 'state', 'msa', 'county'}, default 'us'
        The geographical level of the data.
    state_list: list or 'all', default 'all'
        The list of states to include in the data, identified by postal code 
        abbreviation. (Ex: 'AK', 'UT', etc.) Not available for geo_level = 'us'.
    strata: list, optional
        The variables to stratify the data by.

        --Options--
        * EAGE: Establishment age code
        * EMPSZES: Employment size of establishments code
        * EMPSZESI: Initial employment size of establishments code
        * EMPSZFI: Employment size of firms code
        * EMPSZFII: Initial employment size of firms code
        * FAGE: Firm age code
        * GEOCOMP: GEO_ID Component
        * NAICS: 2017 NAICS Code
        * METRO: Establishments located in Metropolitan or Micropolitan 
            Statistical Area indicator

        FAGE codes
        * 1   Total (0) All firm ages
        * 10  0 Years (1) Firms less than one year old
        * 20  1 Year (1) Firms one year old
        * 25  1-5 Years (2) Firms between one and five years old
        * 30  2 Years (1) Firms two years old
        * 40  3 Years (1) Firms three years old
        * 50  4 Years (1) Firms four years old
        * 60  5 Years (1) Firms five years old
        * 70  6-10 Years (0) Firms between six and ten years old
        * 75  11+ Years (2) Firms eleven or more years old
        * 80  11-15 Years (1) Firms between eleven and fifteen years old
        * 90  16-20 Years (1) Firms between sixteen and twenty years old
        * 100 21-25 Years (1) Firms b/w twenty one and twenty five years old
        * 110 26+ Years (1) Firms twenty six or more years old
        * 150 Left Censored (0) "Firms of unknown age (born before 1977)"

    get_flags: bool, default False
        Whether to include the variables that hold the data flags. Note that the
        information in these variables is already placed inside the original
        variables as a part of the data-cleaning process.
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
    series_list = c.BDS_SERIES if series_list == 'all' else series_list

    state_list = c.STATES if state_list == 'all' else state_list
    state_list = [c.STATE_ABB_TO_FIPS[s] for s in state_list]

    invalid_strata = set(strata) \
        - {'GEOCOMP', 'EAGE', 'EMPSZES', 'EMPSZESI', 'EMPSZFI', 'EMPSZFII', 
            'FAGE', 'NAICS', 'METRO'}
    if invalid_strata:
        raise Exception(
            f'Variables {invalid_strata} are invalid inputs to strata ' \
            'argument. Refer to the function documentation for valid strata.'
        )
    
    if len({'METRO', 'GEOCOMP'} - set(strata)) == 1:
        missing_var = {'METRO', 'GEOCOMP'} - set(strata)
        strata = strata + list(missing_var)
        print(
            'Warning: Variables METRO and GEOCOMP must be used together. ' \
            f'Variable {missing_var} has been added to strata list.')

    # Test that we have a valid strata crossing
    if not check_strata_valid(geo_level, strata):
        raise Exception(
            f'This is not a valid combination of strata for geo_level ' \
            f'{geo_level}. See ' \
            'https://www.census.gov/data/datasets/time-series/econ/bds/bds-datasets.html' \
            ' for a list of valid crossings.'
        )
    
    # Convert coded variables to their labeled versions
    strata = strata + [f'{var}_LABEL' for var in strata if var != 'GEOCOMP']

    # Warn users if they didn't provide a key
    if key == None:
        print('WARNING: You did not provide a key. Too many requests will ' \
            'result in an error.')

    # Data fetch
    if 'NAICS' not in strata or geo_level == 'us':
        url = _bds_url(series_list, geo_level, state_list, strata, key, '*')
        df = api.fetch_from_url(url, requests)
    else:
        years = list(range(1978, 2020))
        df = api.run_in_parallel(
            data_fetch_fn = _bds_fetch_data,
            groups = years,
            constant_inputs = [series_list, geo_level, state_list, strata, key],
            n_threads = n_threads
        )

    flags = [f'{var}_F' for var in series_list] if get_flags else []
    index = ['time', 'fips', 'region', 'geo_level']

    return df \
        .pipe(api._create_fips, geo_level) \
        .rename(columns={
            **{'YEAR': 'time', 'NAICS':'naics'}, 
            **{x:x.lower() for x in strata}
            }
        ) \
        .assign(
            industry=lambda x: x['naics'].map(c.NAICS_CODE_TO_ABB(2)),
            geo_level=geo_level
        ) \
        .apply(
            lambda x: pd.to_numeric(x, errors='ignore') \
                if x.name in series_list + ['time'] else x
        ) \
        .pipe(_mark_flagged, series_list) \
        .sort_values(index + [x.lower() for x in strata]) \
        .reset_index(drop=True) \
        [index + [x.lower() for x in strata] + series_list + flags]