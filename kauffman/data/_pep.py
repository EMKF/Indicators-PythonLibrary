import os
import requests
import numpy as np
import pandas as pd
from kauffman import constants as c
from kauffman.tools import api_tools as api
from kauffman.tools import general_tools as g

# https://www.census.gov/programs-surveys/popest.html

def _fips_region_time(df, geo_level, date_var, date_code_shift, region_var):
    if date_var:
        df['time'] = 2000 + df[date_var].astype(int) + date_code_shift
    else:
        df['time'] = 2021

    if geo_level == 'county':
        return df \
            .query('state != "72"') \
            .assign(
                fips=lambda x: x['state'] + x['county'],
                region=lambda x: x[region_var].str.split(',').str[0]
            )
    else:
        return df \
            .rename(columns={region_var: 'region'}) \
            .assign(fips=lambda x: x['region'].map(c.ALL_NAME_TO_FIPS))


def _fetch_from_api(
    url, geo_level, date_var, date_code_shift, start_year, end_year, region_var,
    pop_var, key
):
    url = url + f'&key={key}' if key else url
    return api.fetch_from_url(url, requests) \
        .rename(columns={pop_var:'population'}) \
        .pipe(
            _fips_region_time, geo_level, date_var, date_code_shift, region_var
        ) \
        [['fips', 'region', 'time', 'population']] \
        .query('region != "Puerto Rico"') \
        .query(f'{start_year} <= time <= {end_year}') \


def _2000_2009(geo_level, key):
    geos = '1' if geo_level == 'us' else '*'
    url = f'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_&for={geo_level}:{geos}'
    return _fetch_from_api(
        url, geo_level, 'DATE_', -2, 2000, 2009, 'GEONAME', 'POP', key
    )


def _2010_2019(geo_level, key):
    url = f'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for={geo_level}:*'
    return _fetch_from_api(
        url, geo_level, 'DATE_CODE', 7, 2010, 2019, 'NAME', 'POP', key
    )


def _2020(geo_level):
    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/' \
        + {'county':'counties', 'state':'state', 'us':'national'}[geo_level] \
        + '/totals/' \
        + ('co-est2020.csv' if geo_level == 'county' else 'nst-est2020.csv')

    if geo_level == 'county':
        df = pd.read_csv(
            url,
            encoding='cp1252',
            dtype={'STATE':str, 'COUNTY':str}
        ) \
        .query('COUNTY != "000"') \
        .assign(fips=lambda x: x['STATE'] + x['COUNTY']) \
        .rename(columns={'CTYNAME': 'region'})
    else:
        df = pd.read_csv(url, dtype={'STATE':str}) \
            .rename(columns={'STATE': 'fips', 'NAME':'region'})
        if geo_level == 'state':
            df = df.query('fips not in ["00", "72"]')
        else:
            df = df.query('region == "United States"')

    return df \
        .rename(columns={'POPESTIMATE2020': 'population'}) \
        .assign(time=2020) \
        [['fips', 'region', 'time', 'population']]


def _2021(geo_level, key):
    url = f'https://api.census.gov/data/2021/pep/population?get=NAME,POP_2021&for={geo_level}:*'
    return _fetch_from_api(
        url, geo_level, None, None, 2021, 2021, 'NAME', 'POP_2021', key
    )


def _format_txt_row(row, i_start, i_end):
    return row[:i_start] + [' '.join(row[i_start: i_end])] + row[i_end:]


def _filter_county_txt(df):
    return df \
        .query('fips != "FIPS"') \
        .query('fips != "Code"') \
        .reset_index(drop=True)


def _county_1980_1989():
    list_1980, list_1985 = [], []
    current_data_year = 1980

    url = 'https://www2.census.gov/programs-surveys/popest/tables/1980-1990/counties/totals/e8089co.txt'
    lines_iter = iter(requests.get(url).text.split('\n')[25:])
    while True:
        row = lines_iter.__next__().split()

        if not row:
            continue
        if row and row[0] == 'FIPS':
            current_data_year = 1985 if current_data_year == 1980 else 1980
        if len(row) < 7:
            row = row + lines_iter.__next__().split()

        if current_data_year == 1980:
            list_1980.append(_format_txt_row(row, 1, -5))
        else:
            list_1985.append(_format_txt_row(row, 1, -5))
            if row[0] == '56045':
                break

    df_1980 = pd.DataFrame(
            list_1980, 
            columns=['fips', 'region'] + _pop_cols(1980, 1984)
        ) \
        .pipe(_filter_county_txt)
    df_1985 = pd.DataFrame(
            list_1985, 
            columns=['fips', 'region'] + _pop_cols(1985, 1989)
        ) \
        .pipe(_filter_county_txt) \
        .drop(columns='region')

    return df_1980 \
        .merge(df_1985, how='left', on='fips') \
        .pipe(pd.wide_to_long, 'population', i='fips', j='time') \
        .query(f'region not in {list(c.STATE_NAME_TO_ABB.keys())}') \
        .assign(
            region=lambda x: x['region'].replace(r'Co\.', 'County', regex=True)
        ) \
        .reset_index(drop=False)


def _county_1990_1999():
    data_list = []
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1990-2000/counties/totals/99c8_00.txt'
    lines = requests.get(url).text.split('\n')[12:3203]
    for line in lines:
        row = line.split()
        if row[1] == '49041':
            data_list.append(
                row[1:7] + [np.nan] + row[10:14] + [' '.join(row[15:])]
            )
        elif row[1] == '50027':
            data_list.append(
                row[1:7] + [np.nan] + row[9:13] + [' '.join(row[14:])]
            )
        else:
            data_list.append(row[1:12] + [' '.join(row[13:])])

    return pd.DataFrame(
            data_list, 
            columns=['fips'] \
                + [f'population{str(year)}' for year in range(1999, 1989, -1)] \
                + ['region']
        ) \
        .pipe(pd.wide_to_long, 'population', i='fips', j='time') \
        .query(f'region not in {list(c.STATE_NAME_TO_ABB.keys())}') \
        .reset_index() \
        .assign(
            population=lambda x: x['population'].replace(',', '', regex=True)
        )
        

def _format_state_txt(df, decade, format_pop=True):
    return df \
        .query(f'time < {decade + 10}') \
        .reset_index() \
        .assign(
            population=lambda x: x['population'] \
                .replace(',', '', regex=True).astype('int') \
                * 1000 if format_pop else x['population'],
            region=lambda x: x['region'].map(c.STATE_ABB_TO_NAME),
            fips=lambda x: x['region'].map(c.ALL_NAME_TO_FIPS)
        ) \
        [['fips', 'region', 'time', 'population']]


def _fetch_state_txt(url, lrange, cols):
    lines = requests.get(url).text.split('\n')
    return pd.DataFrame(
            [line.split() for line in lines[lrange[0]: lrange[1]]], 
            columns=cols
        ) \
        .pipe(pd.wide_to_long, 'population', i='region', j='time')


def _pop_cols(start, end):
    return list(map(lambda x: f'population{x}', range(start, end + 1)))


def _state_1900_1989(decade):
    url_code = {
        **{int(f'19{i}0'):f'st{i}0{i}9ts' for i in range(0,6)},
        **{int(f'19{i}0'):f'st{i}0{i + 1}0ts' for i in range(5,9)}
    }[decade]
    url = f'https://www2.census.gov/programs-surveys/popest/tables/1980-1990/state/asrh/{url_code}.txt'
    line_ranges = {
        **{decade: [(23,72), (82,-1)] for decade in range(1900, 1940, 10)},
        **{
            1940: [(21, 70), (79,-1)],
            1950: [(27, 78), (92, -3)],
            1960: [(24, 75), (86,-1)], 
            1970: [(14, 65), (67, -8)],
            1980: [(11, 62), (70, -1)]
        }
    }[decade]
    line_range1, line_range2 = line_ranges[0], line_ranges[1]
    
    if decade in [1900, 1910, 1920, 1930, 1940]:
        cols1 = ['region'] + _pop_cols(decade, decade + 5)
        cols2 = ['region'] + _pop_cols(decade + 6, decade + 9) 
        format_pop = True
    elif decade in [1950, 1960]:
        cols1 = ['region', 'census'] + _pop_cols(decade, decade + 4)
        cols2 = ['region'] + _pop_cols(decade + 5, decade + 10)
        format_pop = True
    elif decade == 1970:
        cols1 = ['id', 'region'] + _pop_cols(decade, decade + 5)
        cols2 = ['id', 'region'] + _pop_cols(decade + 6, decade + 10)
        format_pop = False
    elif decade == 1980:
        cols1 = ['region'] + _pop_cols(decade, decade + 4)
        cols2 = ['region'] + _pop_cols(decade + 5, decade + 10)
        format_pop = False
    
    return _fetch_state_txt(url, line_range1, cols1) \
        .append(_fetch_state_txt(url, line_range2, cols2)) \
        .pipe(_format_state_txt, decade, format_pop=format_pop)


def _state_1990_1999():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1990-2000/state/totals/st-99-07.txt'
    lines = requests.get(url).text.split('\n')[28: 79]
    return pd.DataFrame(
            [_format_txt_row(line.split(), 2, -11) for line in lines],
            columns=['block', 'fips', 'region'] \
                + list(map(lambda x: f'population{x}', range(1999, 1989, -1))) \
                + ['census']
        ) \
        .drop(['block', 'census'], 1) \
        .pipe(pd.wide_to_long, 'population', i='region', j='time') \
        .reset_index() \
        [['fips', 'region', 'time', 'population']]


def _us_1900_1999():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1900-1980/national/totals/popclockest.txt'
    lines = requests.get(url).text.split('\n')
    return pd.DataFrame(
            [line.split()[2:4] for line in lines[10:-25]], 
            columns=['time', 'population']
        ) \
            .assign(
                population=lambda x: x.population.str.replace(',', ''),
                region='United States',
                fips='00'
            ) \
            .dropna()


def pep(obs_level='us', state_list='all', key=os.getenv("CENSUS_KEY")):
    """
    Fetches and cleans Population Estimates Program (PEP) data from one of two 
    sources, depending on the year and obs_level: 
    (1) The Census's API (https://api.census.gov/data.html, see the pep > 
        population and pep > int_population datasets)
    (2) https://www2.census.gov/programs-surveys/popest

    Parameters
    ----------
    obs_level: {'us', 'state', 'msa', 'county'}, default 'us'
        The geographical level of the data.
    state_list: list or 'all', default 'all'
        The list of states to include in the data, identified by postal code 
        abbreviation. (Ex: 'AK', 'UT', etc.) Not available for obs_level = 'us'.
    key: str, default os.getenv("CENSUS_KEY"), optional
        Census API key. See README for instructions on how to get one, if 
        desired. Otherwise, user can pass key=None, which will work until the
        Census's data limit is exceeded.
    """
    # Warn users if they didn't provide a key
    if key == None:
        print('WARNING: You did not provide a key. Too many requests will ' \
            'result in an error.')

    # state_list
    state_list = c.STATES if state_list == 'all' else state_list
    state_list = [c.STATE_ABB_TO_FIPS[s] for s in state_list]

    # Fetch data
    if obs_level in ['county', 'msa']:
        df = pd.concat(
                [_county_1980_1989(), _county_1990_1999()]
                + [f('county', key) for f in [_2000_2009, _2010_2019]]
                + [_2020('county')]
            ) \
            .assign(
                region=lambda x: x['fips'].map(c.ALL_FIPS_TO_NAME),
                fips_state=lambda x: x.fips.str[0:2]
            ) \
            .query(f'fips_state in {state_list}')
        if obs_level == 'msa':
            df = df \
                .pipe(g.aggregate_county_to_msa, 'fips', ['population']) \
                [['fips', 'region', 'time', 'population']]
    elif obs_level == 'state':
        df = pd.concat(
                [_state_1900_1989(year) for year in range(1900,1981,10)]
                + [_state_1990_1999(), _2020('state')]
                + [f('state', key) for f in [_2000_2009, _2010_2019, _2021]]
            ) \
            .query(f'fips in {state_list}')
    else:
        df = pd.concat(
            [_us_1900_1999(), _2020('us')]
            + [f('us', key) for f in [_2000_2009, _2010_2019, _2021]]
        )

    # TODO: Keep this? Would need to update kese, neb, and eji
    # columns = ['fips', 'region', 'time', 'population']
    # if obs_level in ['county', 'msa']:
    #     columns = ['fips', 'fips_state', 'region', 'time', 'population']

    return df \
        .astype({'population': 'float', 'time': 'int'}) \
        .sort_values(['fips', 'time']) \
        .reset_index(drop=True) \
        [['fips', 'region', 'time', 'population']]