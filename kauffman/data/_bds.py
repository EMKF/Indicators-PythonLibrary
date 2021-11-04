import requests
import pandas as pd
import kauffman.constants as c


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _county_fips(df):
    return df.\
        assign(county=lambda x: x['state'] + x['county']).\
        drop('state', 1)


def _fetch_data(url):
    r = requests.get(url)
    if r.status_code == 200:
        df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
    elif r.status_code == 204:
        df = pd.DataFrame()
    else:
        raise Exception(f'ERROR. Response code: {r.status_code} for url: {url}')
    return df

def _build_url(variables, region, strata, state_fips=None, year='*'):
    var_string = ",".join(variables + strata)
    
    region_string = {
        'us':'us:*',
        'state':f'state:{state_fips}',
        'msa':'metropolitan%20statistical%20area/micropolitan%20statistical%20area:*',
        'county':f'county:*&in=state:{state_fips}'
    }[region]

    naics_string = '&NAICS=00' if 'NAICS' not in strata else ''
    
    return f'https://api.census.gov/data/timeseries/bds?get={var_string}&for={region_string}&YEAR={year}{naics_string}'


def _bds_data_create(variables, region, strata):
    if region in ['us', 'msa']:
        df = _fetch_data(_build_url(variables, region, strata))
    else:
        years = ['*'] if region == 'state' else [y for y in range(1978, 2020)]
        df = pd.concat(
            [
                _fetch_data(_build_url(variables, region, strata, state_fips, year))
                for state_fips in [c.state_abb_to_fips[s] for s in c.states]
                for year in years
            ]
        )

    return df. \
        pipe(lambda x: _county_fips(x) if region == 'county' else x). \
        rename(columns={'county': 'fips', 'state': 'fips', 'us': 'fips', 'YEAR': 'time', 'NAICS': 'naics'}).\
        assign(
            fips=lambda x: '00' if region == 'us' else x['fips'],
            region=lambda x: x['fips'].map(c.all_fips_to_name),
            industry=lambda x: x['naics'].map(c.naics_code_to_abb(2))
        ). \
        apply(lambda x: pd.to_numeric(x, errors='ignore') if x.name not in ['fips', 'naics'] else x).\
        sort_values(['fips', 'time']).\
        reset_index(drop=True) \
        [['fips', 'region', 'time'] + strata + variables]


def bds(series_lst, obs_level='all', strata=[]):
    """ Create a pandas data frame with results from a BDS query. Column order: fips, region, time, series_lst.

    Keyword arguments:

    series_lst-- lst of variables to pull; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf or https://api.census.gov/data/timeseries/bds/variables.html
        # todo: NAICS is always used
        CBSA: Geography
        COUNTY: Geography
        DENOM: (DHS) denominator
        EMP: Number of employees
        ESTAB: Number of establishments
        ESTABS_ENTRY: Number of establishments born during the last 12 months
        ESTABS_ENTRY_RATE: Rate of establishments born during the last 12 months
        ESTABS_EXIT: Number of establishments exited during the last 12 months
        ESTABS_EXIT_RATE: Rate of establishments exited during the last 12 months
        FIRM: Number of firms
        FIRMDEATH_EMP: Number of employees associated with firm deaths during the last 12 months
        FIRMDEATH_ESTABS: Number of establishments associated with firm deaths during the last 12 months
        FIRMDEATH_FIRMS: Number of firms that exited during the last 12 months
        GEO_ID: Geographic identifier code
        INDGROUP: Industry group
        INDLEVEL: Industry level
        JOB_CREATION: Number of jobs created from expanding and opening establishments during the last 12 months
        JOB_CREATION_BIRTHS: Number of jobs created from opening establishments during the last 12 months
        JOB_CREATION_CONTINUERS: Number of jobs created from expanding establishments during the last 12 months
        JOB_CREATION_RATE: Rate of jobs created from expanding and opening establishments during the last 12 months
        JOB_CREATION_RATE_BIRTHS: Rate of jobs created from opening establishments during the last 12 months
        JOB_DESTRUCTION: Number of jobs lost from contracting and closing establishments during the last 12 months
        JOB_DESTRUCTION_CONTINUERS: Number of jobs lost from contracting establishments during the last 12 months
        JOB_DESTRUCTION_DEATHS: Number of jobs lost from closing establishments during the last 12 months
        JOB_DESTRUCTION_RATE: Rate of jobs lost from contracting and closing establishments during the last 12 months
        JOB_DESTRUCTION_RATE_DEATHS: Rate of jobs lost from closing establishments during the last 12 months
        METRO: Establishments located in Metropolitan or Micropolitan Statistical Area indicator
        NATION: Geography
        NET_JOB_CREATION: Number of net jobs created from expanding/contracting and opening/closing establishments during the last 12 months
        NET_JOB_CREATION_RATE: Rate of net jobs created from expanding/contracting and opening/closing establishments during the last 12 months
        REALLOCATION_RATE: Rate of reallocation during the last 12 months
        SECTOR: NAICS economic sector
        STATE: Geography
        SUBSECTOR: Subsector
        SUMLEVEL: Summary Level code
        ucgid: Uniform Census Geography Identifier clause
        YEAR: Year

    strata--list of variables by which to stratify
        GEOCOMP: GEO_ID Component
        EAGE: Establishment age code
        EMPSZES: Employment size of establishments code
        EMPSZESI: Initial employment size of establishments code
        EMPSZFI: Employment size of firms code
        EMPSZFII: Initial employment size of firms code
        FAGE: Firm age code
        NAICS: 2017 NAICS Code

        FAGE codes
            1   Total   0   All firm ages
            10  0 Years 1   Firms less than one year old
            20  1 Year  1   Firms one year old
            25  1-5 Years   2   Firms between one and five years old
            30  2 Years 1   Firms two years old
            40  3 Years 1   Firms three years old
            50  4 Years 1   Firms four years old
            60  5 Years 1   Firms five years old
            70  6-10 Years  0   Firms between six and ten years old
            75  11+ Years   2   Firms eleven or more years old
            80  11-15 Years 1   Firms between eleven and fifteen years old
            90  16-20 Years 1   Firms between sixteen and twenty years old
            100 21-25 Years 1   Firms between twenty one and twenty five years old
            110 26+ Years   1   Firms twenty six or more years old
            150 Left Censored   0   "Firms of unknown age (born before 1977)”

    obs_level--str or lst of the level of observation(s) to pull at.
        all:
        us:
        state:
        county:
        list of regions according to fips code

    first year available is 1978, last year is 2018
    """
    if type(obs_level) == list:
        region_lst = obs_level
    elif obs_level in ['us', 'state', 'county']:
        region_lst = [obs_level]
    else:
        region_lst = ['us', 'state', 'county']

    invalid_strata = set(strata) - {'GEOCOMP', 'EAGE', 'EMPSZES', 'EMPSZESI', 'EMPSZFI', 'EMPSZFII', 'FAGE', 'NAICS'}
    if invalid_strata:
        raise Exception(f'Variables {invalid_strata} are invalid inputs to strata argument. Refer to the function documentation for valid strata.')

    return pd.concat(
            [
                _bds_data_create(series_lst, region, strata)
                for region in region_lst
            ],
            axis=0
        )
