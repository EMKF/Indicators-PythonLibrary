import os
import sys
import requests
import numpy as np
import pandas as pd
import kauffman.constants as c
from kauffman.tools.etl import county_msa_cross_walk as cw

# https://www.census.gov/programs-surveys/popest.html

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 35000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _format_year(df):
    return df.\
        assign(year=lambda x: x['date'].str[:4]).\
        astype({'year': 'int'}). \
        drop('date', 1)


def _format_population(df):
    return df. \
        rename(columns={'value': 'population'}). \
        astype({'population': 'float'}).\
        assign(population=lambda x: x['population'] * 1000)


def _observations_filter(df, start_year, end_year):
    if start_year:
        df.query('year >= {start_year}'.format(start_year=start_year), inplace=True)
    if end_year:
        df.query('year <= {end_year}'.format(end_year=end_year), inplace=True)
    return df.reset_index(drop=True)



def _feature_create(df, region, year_ind=False):
    if year_ind:
        df['year'] = str(1998 + year_ind)
    else:
        df['year'] = (df['DATE_CODE'].astype(int) + 2007).astype(str)

    if region == 'county':
        df['fips'] = df['state'] + df['county']
    elif region == 'msa':
        df['fips'] = df['metropolitan statistical area/micropolitan statistical area']
    elif region == 'state':
        df['fips'] = df['state']
    else:
        df['fips'] = df['us']

    return df


def _feature_keep(df):
    var_lst = ['fips', 'name', 'year', 'population']
    return df[var_lst]


def _county_fetch_data_2000_2009(year):
    url = 'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_DESC&for=county:*&DATE_={0}'.format(year)
    r = requests.get(url)
    return pd.DataFrame(r.json())


def _county_msa_fetch_2010_2019(region):
    if region == 'msa':
        url = 'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'
    elif region == 'county':
        url = 'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for=county:*'
    r = requests.get(url)
    return pd.DataFrame(r.json())


def _county_msa_clean_2010_2019(df, obs_level):
    return df.\
        pipe(_make_header). \
        rename(columns={'POP': 'population', 'GEONAME': 'name', 'NAME': 'name'}). \
        pipe(_feature_create, obs_level). \
        pipe(_feature_keep)


def _json_to_pandas_construct(state_dict):
    return pd.concat(
        [
            pd.DataFrame(values) \
                [['date', 'value']]. \
                pipe(_format_year). \
                pipe(_format_population). \
                assign(fips=c.state_abb_fips_dic[region.upper()])
            for region, values in state_dict.items()
        ]
    )




def _msa_fetch_2004_2009():
    # todo
    """Crosswalks county population data to msa and calculates population for the latter"""
    return cw.get_data(). \
        merge(
            get_data('county'),
            how='left',
            left_on=['time', 'county_fips'],
            right_on=['time', 'fips']
        ). \
        pipe(lambda x: print(x)).\
        sort_values(['msa_fips', 'fips', 'time']).\
        astype({'population': 'int'}) \
        [['population', 'time', 'msa_fips']].\
        groupby(['time', 'msa_fips']).sum().\
        reset_index(drop=False). \
        rename(columns={'msa_fips': 'fips'})



def _url(region):
    # https://www2.census.gov/programs-surveys/popest/tables/1900-1980/state/asrh/e7080sta.txt
    # https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/8090com.txt
    # https://www2.census.gov/programs-surveys/popest/tables/1990-2000/state/totals/st-99-07.txt

    # 2000 through 2009
    # https://www2.census.gov/programs-surveys/popest/tables/2000-2010/intercensal/state/st-est00int-01.csv
    # https://www2.census.gov/programs-surveys/popest/tables/2000-2009/state/totals/nst-est2009-01.csv  # like this one

    # 2010-2019
    # https://www2.census.gov/programs-surveys/popest/tables/2010-2020/state/totals/nst-est2020.xlsx
    # https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx  # like this one

    # 2020
    # ?

    if region == 'us':
        return [
            f'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_&for=us:1',
            f'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for=us:*'
        ]
    else:
        return [
            f'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_&for={region}:*',
            f'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for={region}:*'
        ]

def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:]


# def _us_fips(df):
#     return df.assign(us=)

# def _obs_filter(df, base_year):
#     return df.query('time < 2000')
#     # return df.\
#     #     astype({'date': 'int'}). \
#     #     query('2 <= date <= 11' if ind == 0 else '3 <= date <= 12').\
#     #     query('region not in ["Puerto Rico"]')
#


def _panel_create(df, years):
    return pd.concat(
        [
            df[['region', year]].\
                rename(columns={year: 'POP'}).\
                assign(time=year)
            for year in years
        ],
        axis=0
    )


def _column_rename(df):
    df.columns = [int(col[-4:]) if col != 'region' else col for col in df.columns]
    return df


def _row_format(row):
    return [row[0]] + [' '.join(row[1: -5])] + row[-5:]


def _obs_filter(df):
    return df.\
        query('fips != "FIPS"').\
        query('fips != "Code"').\
        reset_index(drop=True)


def _county_1980_1989():
    lst_1980, lst_1985 = [], []
    lst_1980_bool = 1

    lines_iter = iter(requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/counties/totals/e8089co.txt').text.split('\n')[25:])
    while True:
        row = lines_iter.__next__().split()

        if not row:
            continue
        if row and row[0] == 'FIPS':
            lst_1980_bool = not lst_1980_bool

        if lst_1980_bool:
            if len(row) < 7:
                row = row + lines_iter.__next__().split()
            lst_1980.append(_row_format(row))
        else:
            if len(row) < 7:
                row = row + lines_iter.__next__().split()
            lst_1985.append(_row_format(row))
            if row[0] == '56045':
                break

    df_1980 = pd.DataFrame(lst_1980, columns=['fips', 'region', 'time1980', 'time1981', 'time1982', 'time1983', 'time1984']).\
        pipe(_obs_filter)
    df_1985 = pd.DataFrame(lst_1985, columns=['fips', 'region', 'time1985', 'time1986', 'time1987', 'time1988', 'time1989']).\
        pipe(_obs_filter).\
        drop('region', 1)

    return df_1980.\
        merge(df_1985, how='left', on='fips').\
        pipe(pd.wide_to_long, 'time', i='fips', j='year').\
        query(f'region not in {list(c.state_name_abb_dic.keys())}'). \
        assign(region=lambda x: x['region'].replace(r'Co\.', 'County', regex=True)).\
        reset_index(drop=False).\
        rename(columns={'time': 'population', 'year': 'time'}).\
        astype({'time': 'int', 'population': 'int'})


def _county_1990_1999():
    lst_1990 = []

    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1990-2000/counties/totals/99c8_00.txt').text.split('\n')[12:]
    for line in lines:
        row = line.split()

        if not row:
            break
        if row[1] == '49041':
            lst_1990.append(row[1:7] + [np.nan] + row[10:14] + [' '.join(row[15:])])
        elif row[1] == '50027':
            lst_1990.append(row[1:7] + [np.nan] + row[9:13] + [' '.join(row[14:])])
        else:
            lst_1990.append(row[1:12] + [' '.join(row[13:])])

    return pd.DataFrame(lst_1990, columns=['fips'] + ['time' + str(year) for year in range(1999, 1989, -1)] + ['region']).\
        pipe(pd.wide_to_long, 'time', i='fips', j='year').\
        query(f'region not in {list(c.state_name_abb_dic.keys())}').\
        reset_index().\
        assign(
            population=lambda x: x['time'].replace(',', '', regex=True)
        ).\
        drop('time', 1).\
        rename(columns={'year': 'time'}).\
        astype({'time': 'int', 'population': 'float'})


def _county_2000_2009():
    return pd.concat(
            [
                pd.DataFrame(requests.get('https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_DESC&for=county:*&DATE_={0}'.format(date)).json()). \
                    pipe(_make_header).\
                    query('state != "72"').\
                    assign(
                        time=lambda x: 1998 + x['DATE_'].astype(int),
                        fips=lambda x: x['state'] + x['county'],
                        region=lambda x: x['GEONAME'].str.split(',').str[0]
                    ). \
                    rename(columns={'POP': 'population'}) \
                    [['fips', 'region', 'time', 'population']]
                for date in range(2, 12)
            ],
            axis=0
        ).\
        sort_values(['fips', 'time']).\
        reset_index(drop=True)


def _county_2010_2019():
    return pd.DataFrame(requests.get('https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for=county:*').json()). \
        pipe(_make_header). \
        query('state != "72"'). \
        astype({'DATE_CODE': 'int'}).\
        query('3 <= DATE_CODE').\
        assign(
            time=lambda x: 2007 + x['DATE_CODE'],
            fips=lambda x: x['state'] + x['county'],
            region=lambda x: x['NAME'].str.split(',').str[0]
        ). \
        rename(columns={'POP': 'population'}) \
        [['fips', 'region', 'time', 'population']].\
        sort_values(['fips', 'time']).\
        reset_index(drop=True)


# todo: these functions can be combined. cleanup
#   The first four are identical other than the labels and url.
def _state_1900_1909():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st0009ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[23: 72]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1900, 1906)))). \
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[82:-1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1906, 1910)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1910_1919():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st1019ts_v2.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[23: 72]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1910, 1916)))). \
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[82:-1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1916, 1920)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1920_1929():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st2029ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[23: 72]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1920, 1926)))). \
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[82:-1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1926, 1930)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1930_1939():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st3039ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[23: 72]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1930, 1936)))). \
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[82:-1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1936, 1940)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1940_1949():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st4049ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[21: 70]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1940, 1946)))). \
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[79:-1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1946, 1950)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]



def _state_1950_1959():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st5060ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[27: 78]], columns=['region', 'census'] + list(map(lambda x: f'POP{x}', range(1950, 1955)))). \
            drop('census', 1).\
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[92:-3]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1955, 1961)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        query('time < 1960'). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]



def _state_1960_1969():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st6070ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[24: 75]], columns=['region', 'census'] + list(map(lambda x: f'POP{x}', range(1960, 1965)))). \
            drop('census', 1).\
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[86:-1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1965, 1971)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        query('time < 1970'). \
        assign(
            POP=lambda x: x['POP'].replace(',', '', regex=True).astype(int) * 1000,
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1970_1979():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st7080ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[14: 65]], columns=['id', 'region'] + list(map(lambda x: f'POP{x}', range(1970, 1976)))). \
            drop('id', 1).\
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
        append(
            pd.DataFrame([line.split() for line in lines[67: -8]], columns=['id', 'region'] + list(map(lambda x: f'POP{x}', range(1976, 1981)))). \
                drop('id', 1). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        astype({'POP': 'int'}).\
        query('time < 1980'). \
        assign(
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1980_1989():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/st8090ts.txt').text.split('\n')

    return pd.DataFrame([line.split() for line in lines[11: 62]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1980, 1985)))). \
            pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
            append(
            pd.DataFrame([line.split() for line in lines[70: -1]], columns=['region'] + list(map(lambda x: f'POP{x}', range(1985, 1991)))). \
                pipe(pd.wide_to_long, 'POP', i='region', j='time')
        ). \
        reset_index(drop=False). \
        astype({'POP': 'int'}).\
        query('time < 1990'). \
        assign(
            region=lambda x: x['region'].map(c.state_abb_name_dic),
            fips=lambda x: x['region'].map(c.all_name_fips_dic)
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_1990_1999():
    lines = requests.get('https://www2.census.gov/programs-surveys/popest/tables/1990-2000/state/totals/st-99-07.txt').text.split('\n')

    rows = []
    for row in lines[28: 79]:
        rows.append(
            row.split()[:2] + \
            [' '.join([element for element in row.split() if element not in row.split()[:2] + row.split()[-11:]])] + \
            row.split()[-11:]
        )
    return pd.DataFrame(rows, columns=['block', 'fips', 'region'] + list(map(lambda x: f'POP{x}', range(1999, 1989, -1))) + ['census']).\
        drop(['block', 'census'], 1). \
        pipe(pd.wide_to_long, 'POP', i='region', j='time'). \
        reset_index(drop=False) \
        [['fips', 'region', 'time', 'POP']]


def _state_2000_2009():
    url = 'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_&for=state:*'

    return pd.DataFrame(requests.get(url).json()). \
        pipe(_make_header). \
        rename(columns={'GEONAME': 'region', 'DATE_': 'date'}). \
        astype({'date': 'int'}). \
        query('2 <= date <= 11').\
        query('region not in ["Puerto Rico"]').\
        assign(
            time=lambda x: '200' + (x['date'] - 2).astype(str),
            fips=lambda x: x['region'].map(c.all_name_fips_dic),
        ) \
        [['fips', 'region', 'time', 'POP']]


def _state_2010_2019():
    url = 'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for=state:*'

    return pd.DataFrame(requests.get(url).json()). \
        pipe(_make_header). \
        rename(columns={'NAME': 'region', 'DATE_CODE': 'date'}). \
        astype({'date': 'int'}). \
        query('3 <= date <= 12').\
        query('region not in ["Puerto Rico"]').\
        assign(
            time=lambda x: '20' + (x['date'] + 7).astype(str),
            fips=lambda x: x['region'].map(c.all_name_fips_dic),
        ) \
        [['fips', 'region', 'time', 'POP']]


def _us_1900_1999():
    return pd.read_csv(
            'https://www2.census.gov/programs-surveys/popest/tables/1900-1980/national/totals/popclockest.txt',
            delim_whitespace=True,
            skiprows=9,
            skipfooter=25,
            usecols=[2, 3],
            names=['time', 'POP'],
            converters={'POP': lambda x: x.replace(',', '')},
        ). \
        assign(
            region='United States',
            fips='00'
        )


def _us_2000_2009():
    url = 'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_&for=us:1'

    return pd.DataFrame(requests.get(url).json()). \
        pipe(_make_header). \
        rename(columns={'GEONAME': 'region', 'DATE_': 'date'}). \
        astype({'date': 'int'}). \
        query('2 <= date <= 11').\
        query('region not in ["Puerto Rico"]').\
        assign(
            time=lambda x: '200' + (x['date'] - 2).astype(str),
            fips=lambda x: x['region'].map(c.all_name_fips_dic),
        ) \
        [['fips', 'region', 'time', 'POP']]


def _us_2010_2019():
    url = 'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for=us:*'

    return pd.DataFrame(requests.get(url).json()). \
        pipe(_make_header). \
        rename(columns={'NAME': 'region', 'DATE_CODE': 'date'}). \
        astype({'date': 'int'}). \
        query('3 <= date <= 12').\
        query('region not in ["Puerto Rico"]').\
        assign(
            time=lambda x: '20' + (x['date'] + 7).astype(str),
            fips=lambda x: x['region'].map(c.all_name_fips_dic),
        ) \
        [['fips', 'region', 'time', 'POP']]


def _pep_data_create(region):
    if region == 'county':
        df = pd.concat(
            [
                f() for f in [_county_1980_1989, _county_1990_1999, _county_2000_2009, _county_2010_2019]
            ],
            axis=0
        ).\
            assign(region=lambda x: x['fips'].map(c.all_fips_name_dic))  # todo: can clean region up in the above functions, and also the functions at the return statement below: put those in _county fucntions or below?
    elif region == 'msa':
        df = _pep_data_create('county').\
            pipe(cw, 'fips'). \
            drop(['fips_county', 'region'], 1).\
            groupby(['fips_msa', 'CBSA Title', 'time']).sum().\
            reset_index(drop=False).\
            rename(columns={'CBSA Title': 'region'}). \
            assign(fips=lambda x: x['fips_msa'].astype(int).astype(str)) \
            [['fips', 'region', 'time', 'population']]
    elif region == 'state':
        df = pd.concat(
                [
                    f() for f in
                        [
                            _state_1900_1909, _state_1910_1919, _state_1920_1929, _state_1930_1939, _state_1940_1949,
                            _state_1950_1959, _state_1960_1969, _state_1970_1979, _state_1980_1989, _state_1990_1999,
                            _state_2000_2009, _state_2010_2019
                        ]
                ],
                axis=0
            )
    else:
        df = pd.concat(
            [
                f() for f in [_us_1900_1999, _us_2000_2009, _us_2010_2019]
            ],
            axis=0
        )

    return df. \
        sort_values(['fips', 'time']). \
        reset_index(drop=True).\
        rename(columns={'POP': 'population'}).\
        astype({'population': 'float', 'time': 'int'}) \
        [['fips', 'region', 'time', 'population']]
