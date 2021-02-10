import requests
import numpy as np
import pandas as pd

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _format_covars1(df):
    return df.assign(
            net_change=lambda x: x['net_change'].str.replace(',', ''),
            total_gains=lambda x: x['total_gains'].str.replace(',', ''),
            gross_job_gains_expanding_ests=lambda x: x['gross_job_gains_expanding_ests'].str.replace(',', ''),
            gross_job_gains_opening_ests=lambda x: x['gross_job_gains_opening_ests'].str.replace(',', ''),
            total_losses=lambda x: x['total_losses'].str.replace(',', ''),
            gross_job_losses_contracting_ests=lambda x: x['gross_job_losses_contracting_ests'].str.replace(',', ''),
            gross_job_losses_closing_ests=lambda x: x['gross_job_losses_closing_ests'].str.replace(',', ''),
        ).\
        astype(
            {
                'year': 'int',
                'net_change': 'float',
                'total_gains': 'float',
                'gross_job_gains_expanding_ests': 'float',
                'gross_job_gains_opening_ests': 'float',
                'total_losses': 'float',
                'gross_job_losses_contracting_ests': 'float',
                'gross_job_losses_closing_ests': 'float',
            }
        )


def _to_float(x):
    try:
        return float(x)
    except:
        return np.nan


def _format_covars7(df):
    return df.assign(
            establishments=lambda x: x['establishments'].str.replace(',', ''),
            employment=lambda x: x['employment'].str.replace(',', ''),
            survival_previous_year=lambda x: x['survival_previous_year'].str.replace('_', '').apply(_to_float),  # I have no idea why I can't "astype" this column
        ).\
        astype(
            {
                'year': 'int',
                'establishments': 'int',
                'employment': 'int',
                'survival_since_birth': 'float',
            }
        )

def table1(lines):
    cohort = 1994
    age = 1
    data_lst = []
    for ind, line in enumerate(lines[9:-2]):
        if 'Less than one year' in line:
            data_lst.append([cohort] + ['age 0'] + line.split()[-7:])
        if 'Born before March' in line:
            data_lst.append([cohort] + ['pre 1993'] + line.split()[-7:])
        if 'Total' in line:
            data_lst.append([cohort] + ['total'] + line.split()[-7:])
            cohort += 1
            age = 1
        if '{} year'.format(age) in line:
            data_lst.append([cohort] + ['age {}'.format(age)] + line.split()[-7:])
            age += 1

    return pd.DataFrame(
            data_lst,
            columns=['year', 'age', 'net_change', 'total_gains', 'gross_job_gains_expanding_ests', 'gross_job_gains_opening_ests', 'total_losses', 'gross_job_losses_contracting_ests', 'gross_job_losses_closing_ests']
        ).\
        pipe(_format_covars1)


def table5(lines):
    cohort = 1994
    age = 1
    df_out = pd.DataFrame(columns=['age_class'])
    for ind, line in enumerate(lines[6:-2]):
        if 'Less than one year' in line:
            data_lst = []
            data_lst.append(['Less than one year'] + line.split()[4:])
        if '{} year'.format(age) in line:
            data_lst.append(['{age} {year}'.format(age=age, year='year' if age == 1 else 'years')] + line.split()[2:])
            age += 1
        if 'Born before March' in line:
            data_lst.append(['Born before March 1993'] + line.split()[4:])
        if 'Total' in line:
            data_lst.append(line.split())
            df_out = df_out.merge(pd.DataFrame(data_lst, columns=['age_class'] + list(range(cohort, cohort + len(data_lst[0]) - 1))), how='right', on='age_class')
            age = 1
            cohort += 6
    return df_out


def table7(lines):
    cohort = 1994
    data_lst = []
    for ind, line in enumerate(lines[11:-2]):
        if (not line.split()) or ('openings' in line) or ('ended' in line):
            continue
        data_lst.append([cohort] + line.split()[1:])
        if 'March 2020' in line:  # this is something that needs to be incremented yearly
            cohort += 1

    return pd.DataFrame(
            data_lst,
            columns=['cohort_year', 'year', 'establishments', 'employment', 'survival_since_birth', 'survival_previous_year', 'average_emp']
        ).\
        pipe(_format_covars7).\
        assign(age=lambda x: x['year'] - x['cohort_year'])


def get_data(series_lst, table, industry='00', obs_level='us', start_year=None, end_year=None, seasonally_adj=True, annualize=False):
    # todo: I can't verify that other tables, industries, series, or obs-levels work
    """
        series_lst: str
            bdm: Establishment Age and Survival Data

        table: int,
            1: Private sector gross jobs gains and losses by establishment age
            2: Private sector gross jobs gains and losses, as a percent of employment, by establishment age
            3: Number of private sector establishments by direction of employment change, by establishment age
            4: Number of private sector establishments by direction of employment change by establishment age, as a percent of total establishments
            5: Number of private sector establishments by age
            6: Private sector employment by establishment age
            7: Survival of private sector establishments by opening year
        industry: str, NAICS codes
            11: Agriculture, forestry, fishing, and hunting
            21: Mining, quarrying, and oil and gas extraction
            22: Utilities
            23: Construction
            31: Manufacturing
            42: Wholesale trade
            44: Retail trade
            48: Transportation and warehousing
            51: Information
            52: Finance and insurance
            53: Real estate and rental and leasing
            54: Professional, scientific, and technical services
            55: Management of companies and enterprises
            56: Administrative and waste services
            61: Educational services
            62: Health care and social assistance
            71: Arts, entertainment, and recreation
            72: Accommodation and food services
            81: Other services (except public administration)


    """
    url = 'https://www.bls.gov/{series_lst}/{obs_level}_age_naics_{industry}_table{table}.txt'.format(series_lst=series_lst, obs_level=obs_level, industry=industry, table=table)
    lines = requests.get(url).text.split('\n')

    if table in range(1, 5):
        return table1(lines)
    if table in [5, 6]:
        return table5(lines)
    elif table == 7:
        return table7(lines)


if __name__ == '__main__':
    # df1 = get_data('bdm', 1, '00', 'us')
    # print(df1)
    # df5 = get_data('bdm', 5, '00', 'us')
    # print(df5)
    df7 = get_data('bdm', 7, '00', 'us')
    print(df7.head(30))


# Tables 1 through 4 are similar
# Tables 5 and 6 are similar
# Tables 7 is distinct
