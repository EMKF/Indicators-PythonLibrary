import pandas as pd
from ._pep import pep
import kauffman.constants as c
from kauffman.tools._etl import read_zip

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None


def _col_names_lowercase(df):
    df.columns = df.columns.str.lower()
    return df


def shed_sample_to_pop_weighter(df, year):
    if year == 2014:
        pop_2014 = int(pep(obs_level='us').query('time == 2014')['population'])
        df['pop_weight'] = df['weight3'] / df['weight3'].sum() * pop_2014
    return df


def assign_index(df, year):
    if year == 2014:
        df['ppstaten_adj'] = df['ppstaten'].map(c.state_shed_codes_to_abb)
    elif year in range(2015, 2018):
        df['ppstaten_adj'] = df['ppstaten'].apply(lambda x: x.upper())
    elif year in range(2018, 2021):
        df['ppstaten_adj'] = df['ppstaten']

    return df.assign(
        time=year,
        region=lambda x: x['ppstaten_adj'].map(c.state_abb_to_name),
        fips=lambda x: x['ppstaten_adj'].map(c.state_abb_to_fips)
        )


def _fetch_shed_data(series_lst, year, weight_name):
    print("FETCHING KATIE'S VERSION OF DATA")

    return read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
        pipe(_col_names_lowercase). \
        pipe(shed_sample_to_pop_weighter, year). \
        pipe(assign_index, year). \
        rename(
            columns={
                "caseid": "id",
                "e2": "med_exp_12_months",
                "ppethm": "race_ethnicity",
                "ppage": "age",
                "ppgender": "gender",
                "b2": "man_financially",
                weight_name : "pop_weight"
                }
        ). \
        dropna(subset=['pop_weight']) \
        [['pop_weight', 'fips', 'region', 'time'] + series_lst]


def _shed_data_create(series_lst):
    return pd.concat(
        [
            _fetch_shed_data(series_lst, 2014, 'pop_weight'),
            _fetch_shed_data(series_lst, 2015, "weight3b"),
            _fetch_shed_data(series_lst, 2016, "weight3b"),
            _fetch_shed_data(series_lst, 2017, "weight3b"),
            _fetch_shed_data(series_lst, 2018, "weight2b"),
            _fetch_shed_data(series_lst, 2019, "weight_pop"),
            _fetch_shed_data(series_lst, 2020, "weight_pop"),
        ]
    ).reset_index(drop=True)
    

def shed(series_lst, obs_level='individual'):
# def shed(series_lst, obs_level='individual', strata=[]):
# def shed(obs_level='individual', demographic_lst, series_lst):  
    """
    Create a pandas data frame with results from a SHED query. Column order: fips, region, time, demographic_lst, series_lst.

    Parameters
    ----------
    obs_level : str
        Level of analysis. 
        Options:
            individual: respondent
            us: aggregated to national level using population weights

    demographic_lst : list
        List of demographic variables to be pulled.
        Options:
            gender: male, female

            race_ethnicity: White, Non‐Hispanic, Black, Non‐Hispanic, Other, 
            Non‐Hispanic, Hispanic, 2+ Races, Non‐Hispanic

            age: continuous variable

    series_lst : list
        List of variables to be pulled. 
        Options:
            med_exp_12_months: 'During the past 12 months, have you had any unexpected 
            major medical expenses that you had to pay out of pocket (that were not 
            completely paid for by insurance)?'

            man_financially: 'Which one of the following best describes how well you 
            are managing financially these days?'

    Returns
    -------
    DataFrame
        Output of SHED query.
    """

    # if strata:
    #     if obs_level == 'individual':
    #         obs_level = 'us'

    # return _shed_data_create(obs_level, series_lst, strata)
    return _shed_data_create(obs_level, series_lst)
