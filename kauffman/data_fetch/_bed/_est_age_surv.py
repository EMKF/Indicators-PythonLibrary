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
    vars = [
        'net_change', 'total_gains', 'gross_job_gains_expanding_ests',
        'gross_job_gains_opening_ests', 'total_losses', 
        'gross_job_losses_contracting_ests', 'gross_job_losses_closing_ests'
    ]
    return df \
        .replace('N', np.nan) \
        .replace(',', '', regex=True) \
        .astype({**{'time': 'int'}, **{var: 'float' for var in vars}})


def _format_covars7(df):
    int_vars = ['time', 'end_year', 'establishments', 'employment']
    return df \
        .assign(
            establishments=lambda x: x['establishments'].str.replace(',', ''),
            employment=lambda x: x['employment'].str.replace(',', ''),
            survival_previous_year=lambda x: x['survival_previous_year'] \
                .replace('_', np.NaN).astype('float')  
        ) \
        .astype({
            **{x: 'int' for x in int_vars},
            **{'survival_since_birth': 'float'}
        })


def table1(lines):
    cohort = 1994
    age = 1
    data_list = []
    for line in lines[9:-2]:
        if 'Less than one year' in line:
            data_list.append([cohort] + ['age 0'] + line.split()[-7:])
        if 'Born before March' in line:
            data_list.append([cohort] + ['pre 1993'] + line.split()[-7:])
        if 'Total' in line:
            data_list.append([cohort] + ['total'] + line.split()[-7:])
            cohort += 1
            age = 1
        if f'{age} year' in line:
            data_list.append([cohort] + [f'age {age}'] + line.split()[-7:])
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
    for line in lines[6:-2]:
        if 'Less than one year' in line:
            data_list = []
            data_list.append(['Less than one year'] + line.split()[4:])
        if f'{age} year' in line:
            data_list.append(
                [f'{age} {"year" if age == 1 else "years"}']
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
    for line in lines[11:-2]:
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


def table1bf(df, age=0, size=7):  
    title = 'Table 1-B-F: Annual gross job gains and gross job losses by age ' \
        'and base size of firm'
    title_indices = df.index[df[title] == title].tolist()
    num_rows = title_indices[1] - title_indices[0] - 1
    ind = title_indices[c.BED_AGE_SIZE_LIST.index((age, size))]
    df = df.iloc[(ind - num_rows) + 4:ind]
    last_year_ind = df.index[df[title] == 2021].tolist()[0]
    df = df.loc[:last_year_ind].reset_index(drop=True)
    df.columns = c.BED_TABLE1BF_COLS

    return df \
        .replace('_', np.nan, regex=True) \
        .replace('N', np.nan, regex=True) \
        .replace(',', '', regex=True) \
        .astype(dict(
            zip(c.BED_TABLE1BF_COLS[1:], [float] * len(c.BED_TABLE1BF_COLS[1:]))
        ))


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
