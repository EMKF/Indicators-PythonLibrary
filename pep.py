import sys
import joblib
import requests
import pandas as pd
import constants as c

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def get_data(obs_level, start_year, end_year=None):
    vintage = int(start_year / 10) * 10
    df = pd.DataFrame()



    for date in range(2, 13):
        df_temp = _fetch_data1(date, obs_level).\
            pipe(_make_header).\
            pipe(_feature_create, obs_level, date).\
            rename(columns={'POP': 'population'}).\
            pipe(_feature_keep).\
            query('state != "PR"')
        df = df.append(df_temp).reset_index(drop=True)

    df_temp = _fetch_data2(obs_level).\
        pipe(_make_header).\
        rename(columns={'POP': 'population'}).\
        pipe(_feature_create, obs_level).\
        pipe(_feature_keep).\
        pipe(_observation_filter)

    return df.\
        append(df_temp).\
        sort_values(['state', 'year']).\
        reset_index(drop=True).\
        pipe(_feature_type)


def _United_States_to_US(df):
    df['state'] = 'US'
    return df

def pop_command():
    raw_data_create('state').pipe(h._saver, 'state')
    raw_data_create('us').pipe(_United_States_to_US).pipe(h._saver, 'us')



def _fetch_data1(year, geo_level):
    if geo_level == 'us':
        url = 'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_DESC&for={1}:1&DATE_={0}'.format(year, geo_level)
    else:
        url = 'https://api.census.gov/data/2000/pep/int_population?get=GEONAME,POP,DATE_DESC&for={1}:*&DATE_={0}'.format(year, geo_level)
    print('{0}: {1}'.format(year, url))
    r = requests.get(url)
    return pd.DataFrame(r.json())


def _fetch_data2(geo_level):
    url = 'https://api.census.gov/data/2018/pep/population?get=GEONAME,POP,DATE_CODE&for={0}:*'.format(geo_level)
    print('{0}'.format(url))
    r = requests.get(url)
    return pd.DataFrame(r.json())


def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:, :]


def _feature_create(df, geo_level, year_ind=False):
    if year_ind:
        df['year'] = str(1998 + year_ind)
    else:
        df['year'] = (df['DATE_CODE'].astype(int) + 2007).astype(str)

    if geo_level == 'us':
        df['state'] = df['GEONAME']
    else:
        df['state'] = df['GEONAME'].apply(lambda x: c.us_state_abbrev[x])
    return df


def _feature_keep(df):
    var_lst = ['state', 'year', 'population']
    return df[var_lst]


def _observation_filter(df):
    df['year'] = df['year'].astype(int)
    df.query('year >= 2011', inplace=True)
    df['year'] = df['year'].astype(str)
    return df.query('state != "PR"')

def _feature_type(df):
    df['population'] = df['population'].astype(int)
    return df


def _saver(df, geo_level):
    joblib.dump(df, c.filenamer('data/transformed/pep/yearly_population_{}.pkl'.format(geo_level)))

if __name__ == '__main__':
    df = get_data('state', 2011)
    # df = get_data('us', 2004)
