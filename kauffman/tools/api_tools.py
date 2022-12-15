import pandas as pd
import requests
import re
from kauffman import constants as c
from joblib import Parallel, delayed


def fetch_from_url(url, session):
    success = False
    retries = 0
    while not success and retries < 5:
        try:
            r = session.get(url)
            if r.status_code == 200:
                try:
                    df = pd.DataFrame(r.json()[1:], columns=r.json()[0])
                    success = True
                except:
                    print('Fail for url', url)
                    title = re.compile(r'<title>(.*?)</title>', re.UNICODE) \
                        .search(r.text).group(1)
                    raise Exception(f'error: {title}')
            elif r.status_code == 204:
                df = pd.DataFrame()
                success = True
            elif r.status_code == 400:
                print('Fail. Status code: 400 for url', url)
                raise Exception(r.text)
            else:
               print(f'Fail. Attempt #{retries + 1}/5', 'Status code:', r, url)
               retries += 1
        except Exception as e:
            if str(e).startswith('error'):
                raise e
            else:
                print(f'Fail. Attempt #{retries + 1}/5', e)
                retries += 1
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