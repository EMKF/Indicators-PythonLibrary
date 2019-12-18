import requests
import pandas as pd

# using the jobs_indicators interpreter for now, will need to make its own in the future after I get off of this plane.



def _fetch_data1(sys_level, year, state):
    # todo: integrate state into url
    url = 'https://api.census.gov/data/timeseries/bds/firms?get=net_job_creation,fage4,fsize,firms&for={0}:*&time={1}'.format(sys_level, year)
    print('{0}: {1}'.format(year, url))
    return requests.get(url).json()


def _make_header(df):
    # todo: this might not be necessary
    df.columns = df.iloc[0]
    return df.iloc[1:, :]


def raw_data_to_mongo(start_year, end_year, sys_level='us', state=None):
    for year in range(start_year, end_year + 1):
        r = _fetch_data1(sys_level, year, state)
#         todo: send to mongodb


def raw_data_to_pandas(start_year, end_year, sys_level='us', state=None):
    df = pd.DataFrame()
    for year in range(start_year, end_year):
        df_temp = pd.DataFrame(_fetch_data1(sys_level, year, state)). \
            pipe(_make_header)
        df = df.append(df_temp)
    return df

