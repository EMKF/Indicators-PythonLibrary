import pandas as pd
from ._pep import pep
import kauffman.constants as c
from ..tools.etl import read_zip


import pandas as pd
import kauffman.constants as c
from kauffman.tools.etl import read_zip
# from ..eship_data_sources import pep
import sys

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
# pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None

# issues:
# steps 1) why are there 260 respondents in 2017 without weight3b?
#       2) recode variables to 0-1

def _col_names_lowercase(df):
    df.columns = df.columns.str.lower()
    return df

def _shed_binary_weighter(df):
    print(df.columns)
    # per_country['virus_confirmed'] = per_country.apply(lambda x: x['cases'] if x['type'] == "confirmed" else 0, axis=1)
    return df

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
                [['pop_weight', 'fips', 'region', 'time', ] + series_lst].\
                pipe(_shed_binary_weighter)\
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
    df = _shed_2014(series_lst).\
        append(_shed_2015_2017(series_lst), ignore_index=True).\
        append(_shed_2018(series_lst), ignore_index=True).\
        append(_shed_2019_2020(series_lst),ignore_index=True)
    # df = _shed_binary_weighter(series_lst)
    return df
    # if obs_level == 'us':
    #     return df.groupby(['time']).mean()
    # # to do - change all categorical variables to 0-1
    # # to do - add other aggregating functions? e.g. sum, mean
    # elif obs_level == 'individual':
    #     return df

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
    todo
            - How to use the if-else-elif syntax to filter for region?
                    if type(obs_level) == list:
                        region_lst = obs_level
                    else:
                        if obs_level == 'us':
                            region_lst = ['US']
                        elif obs_level == 'state':
                            region_lst = c.states
                        else:
                            region_lst = ['US'] + c.states
                    --- pull other region variables for region (us, state, region1, region2)
                    --- create us level observation
                    --- will need to apply weights to get to the state, region, national level
                    --- create a function to aggregate based on weights
            - How do we want to handle the weights?
            - Do we want to filter for years?
            - What other inputs do we want?
            Tasks:
            1) get weight variable for final dataset
                - look online for context about weights
                        - didn't find anything helpful
                        - don't think we can use 2013 or 2014 "sample weights"
                        - How do I concat each of the 3 datasets in _shed_data_create()?
                        - How do I pass inputs through _shed_data_create()?
            2) Aggregate to national, reigonal1, regional2, or state level:
                - create a function in etl.py that allows users to specify the name of weight variable and observation level
                    e.g.
                        def survey_aggregate(df, weight_var, obs_level):
                            if obs_level == 'us':
                            #weighted average over all observations
                            elif obs_level == 'state':
                - Use df.groupby('fips').mean() in the above function to get certain statistics
        """

    # if strata:
    #     if obs_level == 'individual':
    #         obs_level = 'us'

    # return _shed_data_create(obs_level, series_lst, strata)
    return _shed_data_create(obs_level, series_lst)

# _shed_data_create(demographic_lst, series_lst)


# def _shed_data_create(demographic_lst, series_lst):
#     return pd.concat(
#         [
#             read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
#                 pipe(_col_names_lowercase).\
#                 assign(
#                     time=year,
#                     fips=lambda x: x['ppstaten'].map(c.shed_state_codes).map(c.state_abb_fips_dic),
#                     region=lambda x: x['ppstaten'].map(c.shed_state_codes).map(c.state_abb_name_dic),
#                     e2=lambda x: x['e2'].map({-1: "refused", 0: "no", 1: "yes"}),
#                     b2=lambda x: x['b2'].map({-1: "refused", 1: "Finding it difficult to get by", 2: "Just getting by", 3: "Doing ok", 4: "Living comfortably"}),
#                     ppethm=lambda x: x['ppethm'].map({1: "White, Non‐Hispanic", 2: "Black, Non‐Hispanic", 3: "Other, Non‐Hispanic", 4: "Hispanic", 5: "2+ Races, Non‐Hispanic"}),
#                     ppgender=lambda x: x['ppgender'].map({1: "Male", 2: "Female", 'Male': "Male", "Female": "Female"})
#                 ). \
#                 rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age", "ppgender": "gender", "b2": "man_financially"}) \
#             [['fips', 'region', 'time',] + demographic_lst + series_lst]
#             for year in range(2013, 2021)
#         ]
#     )
#

