import requests
import numpy as np
import pandas as pd
import kauffman.constants as c


def _data_lines_survival(table, region, industry):
    if region == 'us':
        url = 'https://www.bls.gov/bdm/us_age_naics_' \
            + f'{industry}_table{table}.txt'
    else:
        url = f'https://www.bls.gov/bdm/{region}_age_total_table{table}.txt'
    return requests.get(url).text.split('\n')


def _format_covars1(df):
    return df \
        .replace('N', np.nan) \
        .assign(
            net_change=lambda x: x['net_change'].str.replace(',', ''),
            total_gains=lambda x: x['total_gains'].str.replace(',', ''),
            gross_job_gains_expanding_ests=lambda x: \
                x['gross_job_gains_expanding_ests'].str.replace(',', ''),
            gross_job_gains_opening_ests=lambda x: \
                x['gross_job_gains_opening_ests'].str.replace(',', ''),
            total_losses=lambda x: x['total_losses'].str.replace(',', ''),
            gross_job_losses_contracting_ests=lambda x: \
                x['gross_job_losses_contracting_ests'].str.replace(',', ''),
            gross_job_losses_closing_ests=lambda x: \
                x['gross_job_losses_closing_ests'].str.replace(',', ''),
        ) \
        .astype(
            {
                'time': 'int',
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
    return df \
        .assign(
            establishments=lambda x: x['establishments'].str.replace(',', ''),
            employment=lambda x: x['employment'].str.replace(',', ''),
            survival_previous_year=lambda x: x['survival_previous_year'] \
                .str.replace('_', '').apply(_to_float),  
                # I have no idea why I can't "astype" this column
        ) \
        .astype(
            {
                'time': 'int',
                'end_year': 'int',
                'establishments': 'int',
                'employment': 'int',
                'survival_since_birth': 'float',
            }
        )


def table1(lines):
    cohort = 1994
    age = 1
    data_list = []
    for ind, line in enumerate(lines[9:-2]):
        if 'Less than one year' in line:
            data_list.append([cohort] + ['age 0'] + line.split()[-7:])
        if 'Born before March' in line:
            data_list.append([cohort] + ['pre 1993'] + line.split()[-7:])
        if 'Total' in line:
            data_list.append([cohort] + ['total'] + line.split()[-7:])
            cohort += 1
            age = 1
        if '{} year'.format(age) in line:
            data_list.append(
                [cohort] + ['age {}'.format(age)] + line.split()[-7:]
            )
            age += 1

    return pd.DataFrame(
            data_list,
            columns=[
                'time', 'age', 'net_change', 'total_gains', 
                'gross_job_gains_expanding_ests', 
                'gross_job_gains_opening_ests', 'total_losses', 
                'gross_job_losses_contracting_ests', 
                'gross_job_losses_closing_ests'
            ]
        ) \
        .pipe(_format_covars1)


def table5(lines):
    cohort = 1994
    age = 1
    df_out = pd.DataFrame(columns=['age_class'])
    for ind, line in enumerate(lines[6:-2]):
        if 'Less than one year' in line:
            data_list = []
            data_list.append(['Less than one year'] + line.split()[4:])
        if '{} year'.format(age) in line:
            data_list.append(
                ['{age} {year}'.format(
                    age=age, year='year' if age == 1 else 'years'
                )] \
                + line.split()[2:]
            )
            age += 1
        if 'Born before March' in line:
            data_list.append(['Born before March 1993'] + line.split()[4:])
        if 'Total' in line:
            data_list.append(line.split())
            df_out = df_out.merge(
                pd.DataFrame(
                    data_list, 
                    columns=['age_class'] \
                        + list(range(cohort, cohort + len(data_list[0]) - 1))
                ), how='right', on='age_class'
            )
            age = 1
            cohort += 6
    return df_out


def table7(lines):
    cohort = 1994
    data_list = []
    for ind, line in enumerate(lines[11:-2]):
        if (not line.split()) or ('openings' in line) or ('ended' in line):
            continue
        data_list.append([cohort] + line.split()[1:])
        if 'March 2021' in line:  # this needs to be incremented yearly
            cohort += 1

    return pd.DataFrame(
            data_list,
            columns=[
                'time', 'end_year', 'establishments', 'employment', 
                'survival_since_birth', 'survival_previous_year', 'average_emp'
            ]
        ) \
        .pipe(_format_covars7) \
        .assign(age=lambda x: x['end_year'] - x['time'])


def _extract_rows(df, age, size):
    # todo: make this table title dynamic
    title = 'Table 1-B-F: Annual gross job gains and gross job ' \
        'losses by age and base size of firm'
    
    mask = df[title] == title
    ind_list = df.index[mask].tolist()

    num_rows = ind_list[1] - ind_list[0] - 1
    ind = ind_list[c.BED_AGE_SIZE_LIST.index((age, size))]

    return df.iloc[(ind - num_rows) + 4:ind]


def _remove_trailing_rows(df):
    title = 'Table 1-B-F: Annual gross job gains and gross job ' \
        'losses by age and base size of firm'

    last_year_ind = df.index[df[title] == 2021].tolist()[0]
    return df.loc[:last_year_ind].reset_index(drop=True)


def _column_headers(df):
    df.columns = c.BED_TABLE1BF_COLS
    return df


def _values_fix(df):
    return df \
        .replace('_', np.nan, regex=True) \
        .replace('N', np.nan, regex=True) \
        .replace(',', '', regex=True) \
        .astype(dict(
            zip(c.BED_TABLE1BF_COLS[1:], [float] * len(c.BED_TABLE1BF_COLS[1:]))
        ))


def table1bf(df, age=0, size=7):  
    # todo: make age and size variables in kauffman.bed()
    return df \
        .pipe(_extract_rows, age, size) \
        .pipe(_remove_trailing_rows) \
        .pipe(_column_headers) \
        .pipe(_values_fix)


def est_age_surv_data(table, region, industry):
    if table in range(1, 5):
        df = table1(_data_lines_survival(table, region, industry))
    if table in [5, 6]:
        df = table5(_data_lines_survival(table, region, industry)) \
            .pipe(
                pd.melt, id_vars=['age_class'], var_name='time', 
                value_name='establishments'
            ) \
            .assign(industry=industry) \
            .replace({'_':np.NaN}) \
            [['time', 'industry', 'age_class', 'establishments']]
    if table == 7:
        df = table7(_data_lines_survival(table, region, industry))
    if table == '1bf':
        url = 'https://www.bls.gov/bdm/age_by_size/' \
            + ("" if region == "us" else f"{region}_") \
            + 'age_naics_base_ein_20211_t1.xlsx'
        df = table1bf(pd.read_excel(url, engine='openpyxl'))

    covars = df.columns.tolist()[1:]
    return df \
        .assign(
            region=c.STATE_ABB_TO_NAME[region.upper()],
            fips=c.STATE_ABB_TO_FIPS[region.upper()]
        ) \
        .sort_values(['fips', 'time']) \
        .reset_index(drop=True) \
        [['time', 'fips', 'region'] + covars]
