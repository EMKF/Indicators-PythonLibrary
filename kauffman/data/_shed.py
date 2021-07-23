import pandas as pd
import numpy as np
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


def sample_to_pop_weight(df, year):
    if year in [2013, 2014]:
        weight = c.shed_dic[year]['survey_weight_name']
        pop = c.shed_dic[year]['pop']
        df[weight] = df[weight] / df[weight].sum() * pop
    return df


def format_index(df, year):
    if year in range(2013, 2015):
        df['ppstaten'] = df['ppstaten'].map(c.state_shed_codes_to_abb)

    return df.assign(
        time=year,
        ppstaten=lambda x: x['ppstaten'].apply(lambda x: x.upper()),
        region=lambda x: x['ppstaten'].map(c.state_abb_to_name),
        fips=lambda x: x['ppstaten'].map(c.state_abb_to_fips)
        )


def _fetch_shed_data(series_lst, year):
    weight_name = c.shed_dic[year]['survey_weight_name']

    return read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
        pipe(_col_names_lowercase). \
        pipe(sample_to_pop_weight, year). \
        pipe(format_index, year). \
        rename(columns={
                **{
                    "caseid": "id",
                    "ppage": "age",
                    "ppgender": "gender",
                    "ppcm0160": "occupation",
                    "ppethm": "race_ethnicity",
                    "ppeduc": "highest_educ",
                    "e2": "med_exp_12_months",
                    "b2": "man_financially",
                    "ppwork": "work_status",
                    "ppcm0062": "num_jobs",
                    "i9": "income_variance",
                    weight_name : "pop_weight"
                },
                **c.shed_dic[year]['survey_to_col_name']
            }
        ). \
        dropna(subset=['pop_weight']) \
        [['fips', 'region', 'time', 'pop_weight'] + series_lst]


def _shed_data_create(series_lst):
    return pd.concat(
        [
            _fetch_shed_data(series_lst, year)
            for year in range(2013, 2021)
        ]
    ).reset_index(drop=True)
    

def shed(series_lst, obs_level='individual'):
# def shed(series_lst, obs_level='individual', strata=[]):
# def shed(obs_level='individual', demographic_lst, series_lst):  
    """
    Create a pandas data frame with results from a SHED query. Column order: fips, region, time, demographic_lst, series_lst.

    Parameters
    ----------
    obs_level: str
        individual: respondent
        us: aggregated to national level using population weights

    demographic_lst: list
        gender: male, female

        race_ethnicity: White, Non‐Hispanic, Black, Non‐Hispanic, Other, 
        Non‐Hispanic, Hispanic, 2+ Races, Non‐Hispanic

        age: continuous variable

    series_lst: list
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
    return _shed_data_create(series_lst)
