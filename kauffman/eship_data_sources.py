import pandas as pd
import kauffman.constants as c
from kauffman.helpers import _bfs_data_create, _bds_data_create, _pep_data_create


# todo: updates (1) move the column and renaming lines to _helpers files and reindenxing.
# todo: mostly the code in each of these is the same...so can consolidate that
def bfs(series_lst, obs_level='all', seasonally_adj=True, annualize=False, march_shift=False):
    """ Create a pandas data frame with results from a BFS query. Column order: fips, region, time, series_lst.


    Keyword arguments:
    series_lst-- lst of variables to be pulled.

        Variables:
            BA_BA: 'Business Applications'
            BA_CBA: 'Business Applications from Corporations'
            BA_HBA: 'High-Propensity Business Applications'
            BA_WBA: 'Business Applications with Planned Wages'
            BF_BF4Q: 'Business Formations within Four Quarters'
            BF_BF8Q: 'Business Formations within Eight Quarters'
            BF_PBF4Q: Projected Business Formations within Four Quarters
            BF_PBF8Q: Projected Business Formations within Eight Quarters
            BF_SBF4Q: Spliced Business Formations within Four Quarter
            BF_SBF8Q: Spliced Business Formations within Eight Quarters
            BF_DUR4Q: Average Duration (in Quarters) from Business Application to Formation within Four Quarters
            BF_DUR8Q: Average Duration (in Quarters) from Business Application to Formation within Eight Quarters

    obs_level-- The level to pull observations for. ('state', 'us', or 'all')


    seasonally_adj-- Option to use the census adjustment for seasonality and smooth the time series. (True or False)

    annualize-- Aggregates across months and annulizes data. (True or False)

    march_shift-- When True the year end is March, False the year end is December. (True or False)



    """

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level == 'us':
            region_lst = ['US']
        elif obs_level == 'state':
            region_lst = c.states
        else:
            region_lst = ['US'] + c.states

    return pd.concat(
            [
                _bfs_data_create(region, series_lst, seasonally_adj, annualize, march_shift)
                for region in region_lst
            ],
            axis=0
        ).\
        reset_index(drop=True)

def bds(series_lst, obs_level='all'):
    """ Create a pandas data frame with results from a BDS query. Column order: fips, region, time, series_lst.

    Keyword arguments:

    series_lst-- lst of variables to pull; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf or https://api.census.gov/data/timeseries/bds/variables.html

        CBSA: Geography
        COUNTY: Geography
        DENOM: (DHS) denominator
        EAGE: Establishment age code
        EMP: Number of employees
        EMPSZES: Employment size of establishments code
        EMPSZESI: Initial employment size of establishments code
        EMPSZFI: Employment size of firms code
        EMPSZFII: Initial employment size of firms code
        ESTAB: Number of establishments
        ESTABS_ENTRY: Number of establishments born during the last 12 months
        ESTABS_ENTRY_RATE: Rate of establishments born during the last 12 months
        ESTABS_EXIT: Number of establishments exited during the last 12 months
        ESTABS_EXIT_RATE: Rate of establishments exited during the last 12 months
        FAGE: Firm age code
        FIRM: Number of firms
        FIRMDEATH_EMP: Number of employees associated with firm deaths during the last 12 months
        FIRMDEATH_ESTABS: Number of establishments associated with firm deaths during the last 12 months
        FIRMDEATH_FIRMS: Number of firms that exited during the last 12 months
        GEO_ID: Geographic identifier code
        GEOCOMP: GEO_ID Component
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
        NAICS: 2012 NAICS Code
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
    ????
    https://www.census.gov/econ/bfs/csv/bfs_us_apps_weekly_nsa.csv
    from https://www.census.gov/econ/bfs/index.html?#
    dictionary: https://www.census.gov/econ/bfs/pdf/bfs_weekly_data_dictionary.pdf
    #todo clean this part up. Travis added these so he would know what the question mark is for

    obs_level-- str or lst of the level of observation(s) to pull at.
            all:
            us:
            state:
            county:
            list of regions according to fips code

    first year available is 1978, last year is 2018
    """
    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level in ['us', 'state', 'county']:
            region_lst = [obs_level]
        else:
            region_lst = ['us', 'state', 'county']

    return pd.concat(
            [
                _bds_data_create(series_lst, region)
                for region in region_lst
            ],
            axis=0
        ). \
        reset_index(drop=True) \
        [['fips', 'region', 'time'] + series_lst]


def pep(obs_level, start_year=None, end_year=None):
    """ Create a pandas data frame with results from a PEP query. Column order: fips, region, time, POP.

    Collects nation- and state-level population data, similar to https://fred.stlouisfed.org/series/CAPOP, from FRED. Requires an api key...
    register here: https://research.stlouisfed.org/useraccount/apikey. For now, I'm just including my key until we
    figure out the best way to do this.

    #todo travis edit ^

    Collects county-level population data from the Census API:

    (as of 2020.03.16)

    Keyword arguments:

    obs_level-- str of the level of observation to pull from.
        'state': resident population of state from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019

    start_year-- earliest start year is 1900

    end_year-- latest end year is 2019
    """

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level in ['us', 'state', 'county']:
            region_lst = [obs_level]
        else:
            region_lst = ['us', 'state', 'county']

    return pd.concat(
            [
                _pep_data_create(region)
                for region in region_lst
            ],
            axis=0
        ). \
        reset_index(drop=True) \
        [['fips', 'region', 'time', 'POP']]
