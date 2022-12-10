import pandas as pd
import requests
from kauffman import constants as c
from joblib import Parallel, delayed


def fetch_from_url(url, session):
    success = False
    retries = 0
    while not success and retries < 5:
        try:
            r = session.get(url)
            if r.status_code == 200:
                df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
                success = True
            elif r.status_code == 204:
                df = pd.DataFrame()
                success = True
            else:
               print(f'Fail. Retry #{retries}', 'Status code:', r, url)
               retries += 1
               df = pd.DataFrame()
        except Exception as e:
            print(f'Fail. Retry #{retries}', e)
            retries += 1
            df = pd.DataFrame()
    if not success:
        raise Exception(f'Maxed out retries with url: {url}')
    return df


def run_in_parallel(data_fetch_function, groups, constant_inputs, n_threads):
    s = requests.Session()
    parallel = Parallel(n_jobs=n_threads, backend='threading')

    with parallel:
        df = pd.concat(
            parallel(
                delayed(data_fetch_function)(g, *constant_inputs, s)
                for g in groups
            )
        )

    s.close()
    return df


def create_fips(df, obs_level):
    if obs_level == 'state':
        df['fips'] = df['state'].astype(str)
    elif obs_level == 'county':
        df['fips'] = df['state'].astype(str) + df['county'].astype(str)
    elif obs_level == 'msa':
        df['fips'] = df[c.API_MSA_STRING].astype(str)
    else:
        df = df.assign(fips='00')
    return df.assign(region=lambda x: x['fips'].map(c.ALL_FIPS_TO_NAME))