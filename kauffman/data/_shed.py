import pandas as pd
import numpy as np
import kauffman.constants as c
from kauffman.tools._etl import read_zip


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


def select_cols(df, strata, series_lst):
    return df[
        [var for var in strata if var in df.columns]
        + [var for var in series_lst if var in df.columns]
        + ['pop_weight']
    ]


def _fetch_shed_data(series_lst, year, strata):
    weight_name = c.shed_dic[year]['survey_weight_name']

    return read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']). \
        pipe(_col_names_lowercase). \
        pipe(sample_to_pop_weight, year). \
        pipe(format_index, year). \
        rename(columns={
                **{weight_name : "pop_weight"},
                **c.shed_dic[year]['survey_to_col_name'],
                **c.shed_dic['survey_to_col_name_const']
            }
        ). \
        dropna(subset=['pop_weight']). \
        pipe(select_cols, strata, series_lst)


def _shed_data_create(series_lst, strata):
    return pd.concat(
        [
            _fetch_shed_data(series_lst, year, strata)
            for year in range(2013, 2021)
        ]
    ).reset_index(drop=True)
    

def shed(series_lst='all', strata=[]):
    """
    Create a pandas data frame with results from a SHED query. Column order: fips, 
    region, time, strata, series_lst.

    Parameters
    ----------
    series_lst: list
        Below is a list of variables that this code fetches and their corresponding 
        question in the original survey. Bullet points give the variable name from the 
        original survey. More complete documentation may be found in SHED Codebooks at 
        the following link: 
        https://www.federalreserve.gov/consumerscommunities/shed_data.htm

        Variables: Survey labels
        --------
        man_financially: "Overall, which one of the following best describes how well 
        you are managing financially these days?"
            * 2013-2020: 'B2'

        better_off_financially:	"Compared to 12 months ago, would you say that you (and 
        your family) are better off, the same, or worse off financially?"
            * 2013: Missing (different phrasing) |  2014-2020: 'B3'
        
        rainy_day_saving: "Have you set aside emergency or rainy day funds that would 
        cover your expenses for 3 months in case of sickness, job loss, economic 
        downturn, or other emergencies?"
            * 2013-2014: 'E1B' | 2015-2020: 'EF1'

        emergency_covered:	"If you were to lose your main source of income (for 
        example, job or government benefits), could you cover your expenses for 3 
        months by borrowing money, using savings, or selling assets?"
            * 2013-2014: 'E1A' | 2015-2020: 'EF2'

        applied_credit: "In the past 12 months, have you (or your spouse/or your 
        partner) applied for any credit (such as a credit card, higher credit card 
        limit, mortgage, refinance, student loan, personal loan, or other loan)?"
            * 2013: 'S12' | 2014-2020: 'A0'

        has_bank_account: "Do you [and/or your spouse / and/or your partner] currently 
        have a checking, savings, or money market account?"
            * 2013: 'S1A' | 2014: 'D7' | 2015-2020: 'BK1'

        rent: "About how much do you pay for rent each month?"
            * 2013: 'R3A' | 2014-2020: 'R3'

        tot_income:	"Which of the following categories best describes the total income 
        that you (and your spouse/partner) received from all sources, before taxes and 
        deductions, in the past 12 months?"
            * 2013: 'I4' | 2014: Missing | 2015-2016: 'I4A' | 2017-2020: 'I40'

        income_variance: "In the past 12 months, which one of the following best 
        describes your [and/or your spouse/parnter] income?"
            * 2013: 'I9' | 2014: Missing | 2015-2020: 'I9'

        own_business_retirement: "[Own a business or real estate that will provide 
        income in retirement] Do you currently have each of the following types of 
        retirement savings?"
            * 2013: Missing | 2014: 'K2_g' | 2015-2018: 'K2_f' | 2019-2020: Missing
        
        schedule_variance: "Still thinking about your main job, do you normally start 
        and end work around the same time each day that you work or does it vary?"
            * 2013-2015: Missing | 2016: 'D3A' | 2017-2020: 'D30'

        num_jobs: "Altogether, how many jobs do you have?"
            * 2013-2015: Missing | 2016-2020: 'ppcm0062'

        self_emp_income	: "[Self-employment] In the past 12 months, did you and/or your
        spouse/partner] receive any income from the following sources?"
            * 2013-2014: Missing | 2015-2018: 'I0_b' | 2019-2020: Missing (different
            phrasing)

    strata: list
        gender: male, female
            * 2013: 'PPGENDER' | 2014-2020: 'ppgender'

        race_ethnicity: White, Non‐Hispanic, Black, Non‐Hispanic, Other, 
        Non‐Hispanic, Hispanic, 2+ Races, Non‐Hispanic
            * 2013: 'PPETHM' | 2014-2020: 'ppethm'

        agegroup: 18-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75+
            * 2013: 'PPAGECAT' | 2014-2020: 'ppagecat'

        education: Less than high school, High school, Some college, Bachelor's degree
        or higher
            * 2013: 'PPEDUCAT' | 2014-2020: 'ppeducat'

        occupation: See online for list of values
            * 2013: Missing | 2014-2020: ppcm0160

    Returns
    ------- 
    DataFrame
        Output of SHED query.
    """
    series_lst = c.shed_outcomes if series_lst == 'all' else series_lst
    strata = ['fips', 'region', 'time'] + strata

    return _shed_data_create(series_lst, strata)
