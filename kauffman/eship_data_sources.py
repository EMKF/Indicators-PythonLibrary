import pandas as pd
import kauffman.constants as c
from kauffman.helpers import _bfs_data_create, _bds_data_create, _pep_data_create


def bfs(series_lst, obs_level='all', seasonally_adj=True, annualize=False):
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
                _bfs_data_create(region, series_lst, seasonally_adj, annualize)
                for region in region_lst
            ],
            axis=0
        ).\
        reset_index(drop=True) \
        [['fips', 'region', 'time'] + series_lst]


def bds(series_lst, obs_level='all'):
    """
    series_lst: lst; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf or https://api.census.gov/data/timeseries/bds/variables.html
        FIRM: Number of firms
        FAGE: Firm age code
        NET_JOB_CREATION: Number of net jobs created from expanding/contracting and opening/closing establishments during the last 12 months

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



    # if obs_level == 'state':
    #     region_dict = {state: h._state_us_fetch_data_all(state) for state in c.states}
    #     df = h._json_to_pandas_construct(region_dict)
    #
    # elif obs_level == 'us':
    #     region_dict = {'us': h._state_us_fetch_data_all('us')}
    #     df = h._json_to_pandas_construct(region_dict)
    #
    # elif obs_level == 'county':
    #     df = pd.concat(
    #             [
    #                 h._county_fetch_data_2000_2009(date). \
    #                     pipe(h._make_header). \
    #                     pipe(h._feature_create, obs_level, date). \
    #                     rename(columns={'POP': 'population', 'GEONAME': 'name'}). \
    #                     pipe(h._feature_keep)
    #                 for date in range(2, 12)
    #             ]
    #         ).\
    #         append(
    #             h._county_msa_fetch_2010_2019(obs_level).pipe(h._county_msa_clean_2010_2019, obs_level)
    #         ).\
    #         sort_values(['fips', 'year'])
    #
    # elif obs_level == 'msa':
    #     df = h._msa_fetch_2004_2009().\
    #         append(
    #             h._county_msa_fetch_2010_2019(obs_level).pipe(h._county_msa_clean_2010_2019, obs_level).rename(columns={'year': 'time'})
    #         ).\
    #         sort_values(['fips', 'time'])
    #
    # return df.\
    #     pipe(h._observations_filter, start_year, end_year).\
    #     rename(columns={'year': 'time'}). \
    #     drop_duplicates(['fips', 'time'], keep='first'). \
    #     reset_index(drop=True) \
    #     [['fips', 'time', 'population']]


