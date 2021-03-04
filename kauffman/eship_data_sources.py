import pandas as pd
import kauffman.constants as c
from kauffman.helpers import _bfs_data_create, _bds_data_create, _pep_data_create


# todo: updates (1) move the column and renaming lines to _helpers files and reindenxing.
# todo: mostly the code in each of these is the same...so can consolidate that
def bfs(series_lst, obs_level='all', seasonally_adj=True, annualize=False, march_shift=False):
    """
    series_lst: lst

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
    """
    series_lst: lst; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf or https://api.census.gov/data/timeseries/bds/variables.html
        FIRM: Number of firms
        FAGE: Firm age code
        NET_JOB_CREATION: Number of net jobs created from expanding/contracting and opening/closing establishments during the last 12 months

    ????
    https://www.census.gov/econ/bfs/csv/bfs_us_apps_weekly_nsa.csv
    from https://www.census.gov/econ/bfs/index.html?#
    dictionary: https://www.census.gov/econ/bfs/pdf/bfs_weekly_data_dictionary.pdf


        obs_level: str or lst
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
    """
    Collects nation- and state-level population data, similar to https://fred.stlouisfed.org/series/CAPOP, from FRED. Requires an api key...
    register here: https://research.stlouisfed.org/useraccount/apikey. For now, I'm just including my key until we
    figure out the best way to do this.

    Collects county-level population data from the Census API:

    (as of 2020.03.16)
    obs_level:
        'state': resident population of state from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019

    start_year: earliest start year is 1900

    end_year: latest end year is 2019
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
