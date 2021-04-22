import os
import sys
import requests
import pandas as pd
import kauffman.constants as c
# import kauffman.tools.cross_walk as cw  # todo fix

# https://www.census.gov/programs-surveys/popest.html

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
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
    df.columns = df.iloc[0].tolist()
    return df.iloc[1:]


# def _us_fips(df):
#     return df.assign(us=)

def _obs_filter(df, base_year):
    return df.query('time < 2000')
    # return df.\
    #     astype({'date': 'int'}). \
    #     query('2 <= date <= 11' if ind == 0 else '3 <= date <= 12').\
    #     query('region not in ["Puerto Rico"]')



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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
            region=lambda x: x['region'].map(c.abb_name_dic),
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
    if region == 'state':
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
        astype({'POP': 'int', 'time': 'int'}) \
        [['fips', 'region', 'time', 'POP']]
