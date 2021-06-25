import sys
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

# issues:
# steps 1) why are there 260 respondents in 2017 without weight3b?
#       2) recode variables to 0-1


def _col_names_lowercase(df):
    df.columns = df.columns.str.lower()
    return df

# def _shed_binary_weighter(df):
#     print(df.columns)
#     # per_country['virus_confirmed'] = per_country.apply(lambda x: x['cases'] if x['type'] == "confirmed" else 0, axis=1)
#     return df
#
def shed_sample_to_pop_weighter(df):
    pop_2014 = int(pep(obs_level='us').query('time == 2014')['population'])
    df['pop_weight'] = df['weight3'] / df['weight3'].sum() * pop_2014
    return df

def _shed_2014(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                pipe(shed_sample_to_pop_weighter). \
                assign(
                    time=year,
                    abb_region=lambda x: x['ppstaten'].map(c.state_shed_codes_to_abb),
                    region=lambda  x: x['abb_region'].map(c.state_abb_to_name),
                    fips=lambda x: x['abb_region'].map(c.state_abb_to_fips),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2014, 2015)
        ]
    )


def _shed_2015_2017(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    upper=lambda x: x['ppstaten'].apply(lambda x: x.upper()),
                    region=lambda x: x['upper'].map(c.state_abb_to_name),
                    fips=lambda x: x['upper'].map(c.state_abb_to_fips),
                ). \
                dropna(subset=['weight3b']).\
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight3b": "pop_weight"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2015, 2018)
        ]
    )


def _shed_2018(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    fips=lambda x: x['ppstaten'].map(c.state_abb_to_fips),
                    region=lambda x: x['ppstaten'].map(c.state_abb_to_name),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight2b": "pop_weight"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2018, 2019)
        ]
    )


def _shed_2019_2020(series_lst):
    return pd.concat(
        [
            read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
                pipe(_col_names_lowercase). \
                assign(
                    time=year,
                    fips=lambda x: x['ppstaten'].map(c.state_abb_to_fips),
                    region=lambda x: x['ppstaten'].map(c.state_abb_to_name),
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age",
                                "ppgender": "gender", "b2": "man_financially", "weight_pop": "pop_weight"}) \
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst]
            for year in range(2019, 2021)
        ]
    )



# def _shed_data_create(obs_level, series_lst, strata):
def _shed_data_create(obs_level, series_lst):
    return _shed_2014(series_lst).\
        append(_shed_2015_2017(series_lst), ignore_index=True).\
        append(_shed_2018(series_lst), ignore_index=True).\
        append(_shed_2019_2020(series_lst),ignore_index=True)

def shed(series_lst, obs_level='individual'):
# def shed(series_lst, obs_level='individual', strata=[]):
# def shed(obs_level='individual', demographic_lst, series_lst):
    """ Create a pandas data frame with results from a SHED query. Column order: fips, region, time, demographic_lst, series_lst.
    Keyword arguments:
        obs_level-- input for level of analysis
            values:
                individual: respondent
                us: aggregated to national level using population weights
        demographic_lst-- list of demographic variables to be pulled todo -
            Demographic variables:
                gender: male, female
                race_ethnicity: White, Non‐Hispanic, Black, Non‐Hispanic, Other, Non‐Hispanic, Hispanic, 2+ Races, Non‐Hispanic
                age: continuous variable
        series_lst-- lst of variables to be pulled.
            Variables:
                med_exp_12_months: 'During the past 12 months, have you had any unexpected major medical expenses that
                                    you had to pay out of pocket (that were not completely paid for by insurance)?'
                man_financially: 'Which one of the following best describes how well you are managing financially these days?'
        """

    # if strata:
    #     if obs_level == 'individual':
    #         obs_level = 'us'

    # return _shed_data_create(obs_level, series_lst, strata)
    return _shed_data_create(obs_level, series_lst)
