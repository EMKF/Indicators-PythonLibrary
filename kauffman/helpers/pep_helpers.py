import requests
import pandas as pd
from scratch import constants as c
import kauffman.cross_walk as cw

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


def _state_us_fetch_data_all(region):
    if region == 'us':
        series_id = 'B230RC0A052NBEA'  # yearly data
        # series_id = 'POPTHM'  # monthly data
    else:
        series_id = region + 'POP'

    url = 'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={key}&file_type=json'.format(
        series_id=series_id, key='b6602eab475fc27e3ea2feaedd7ff81b')
    r = requests.get(url)
    return r.json()['observations']


def _msa_fetch_2004_2009():
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




def _make_header(df):
    df.columns = df.iloc[0].tolist()
    return df.iloc[1:]


def _obs_filter(df, ind):
    return df.\
        astype({'date': 'int'}). \
        query('2 <= date <= 11' if ind == 0 else '3 <= date <= 12')


def _pep_data_create(region):
    return pd.concat(
            [
                pd.DataFrame(requests.get(url).json()). \
                    pipe(_make_header). \
                    rename(columns={'NAME': 'region', 'GEONAME': 'region', 'state': 'fips', 'DATE_': 'date', 'DATE_CODE': 'date'}). \
                    pipe(_obs_filter, ind). \
                    assign(time=lambda x: '200' + (x['date'] - 2).astype(str) if ind == 0 else '20' + (x['date'] + 7).astype(str)) \
                    [['fips', 'region', 'time', 'POP']]
                for ind, url in enumerate([f'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_&for={region}:*', f'https://api.census.gov/data/2019/pep/population?get=NAME,POP,DATE_CODE&for={region}:*'])
            ],
            axis=0
        ). \
        astype({'POP': 'int', 'time': 'int'}). \
        sort_values(['fips', 'time'])

