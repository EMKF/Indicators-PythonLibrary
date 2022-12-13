import requests
import pandas as pd
from joblib import Parallel, delayed
from kauffman import constants as c
from kauffman.tools._etl import CBSA_crosswalk


def _get_state_release_info(state, session):
    url = 'https://lehd.ces.census.gov/data/qwi/latest_release/' \
        f'{state}/version_qwi.txt'
    r = session.get(url)
    if r.status_code == 200:
        content = r.text.split('\n')
        versions = [content[i].split(' ')[5] for i in range(0,3)]
        dates = [content[i].split(' ')[6].split('_')[2] for i in range(0,3)]

        if len(set(versions)) == 1 and len(set(dates)) == 1:
            return pd.DataFrame(
                [[state, versions[0], dates[0]]], 
                columns=['state', 'latest_release', 'date']
            )
        else:
            print('Warning: Multiple data versions for state ', state)
            return pd.DataFrame(
                [[state, versions, dates]], 
                columns=['state', 'latest_release', 'date']
            )
    else:
        print('ERROR for state ', state, ': r = ', r)


def latest_releases(state_list, n_threads):
    s = requests.Session()
    parallel = Parallel(n_jobs=n_threads, backend='threading')
    with parallel:
        df = pd.concat(
            parallel(
                delayed(_get_state_release_info)(state.lower(), s)
                for state in state_list
            )
        )
    s.close()

    return df \
        .assign(
            state=lambda x: x.state.str.upper(),
            date=lambda x: pd.to_datetime(x.date, format='%Y%m%d')
        ) \
        .sort_values('date') \
        .reset_index(drop=True)


def consistent_releases(state_list='all', n_threads=30, enforce=False):
    state_list = c.STATES if state_list == 'all' else state_list

    df_releases = latest_releases(state_list, n_threads)
    if df_releases.latest_release.nunique() > 1:
        if enforce:
            releases_dict = dict(
                df_releases.groupby('latest_release')['state'].apply(list)
            )
            raise Exception(
                'There are multiple releases currently in use:',
                list(releases_dict.keys()), '\n',
                f'Here are the corresponding states: {releases_dict}'     
            )
        return False
    return True


def estimate_data_shape(
    indicator_list, obs_level_lst, firm_char, worker_char, strata_totals, 
    state_list, fips_list
):
    n_columns = len(
        indicator_list + firm_char + worker_char \
        + ['time', 'fips', 'region', 'ownercode', 'geo_level']
    )
    row_estimate = 0
    state_to_years = c.QWI_START_TO_END_YEAR()

    for level in obs_level_lst:
        if level == 'us':
            year_regions = state_to_years['00']['end_year'] \
                - state_to_years['00']['start_year'] + 1
        elif level == 'state':
            year_regions = pd.DataFrame(state_to_years) \
                .T.reset_index() \
                .rename(columns={'index':'state'}) \
                .query(f'state in {state_list}') \
                .assign(n_years=lambda x: x['end_year'] - x['start_year'] + 1) \
                ['n_years'].sum()
        else:
            query = f"fips_{level} in {fips_list}" if fips_list \
                else f"fips_state in {state_list}"
            year_regions = CBSA_crosswalk() \
                .query(query) \
                [[f'fips_{level}', 'fips_state']] \
                .drop_duplicates() \
                .groupby('fips_state').count() \
                .reset_index() \
                .assign(
                    n_years=lambda x: x['fips_state'] \
                        .map(
                            lambda state: state_to_years[state]['end_year'] \
                                - state_to_years[state]['start_year'] + 1
                        ),
                    year_regions=lambda x: x['n_years']*x[f'fips_{level}']
                ) \
                ['year_regions'].sum()

        # Get n_strata_levels
        strata_levels = 1
        strata = worker_char + firm_char
        strata_to_nlevels = c.QWI_STRATA_TO_NLEVELS
        if not strata_totals:
            strata_to_nlevels = {k:v - 1 for k,v in strata_to_nlevels.items()}
        for s in strata:
            strata_levels *= strata_to_nlevels[s]
        
        row_estimate += year_regions*strata_levels*4

    return (row_estimate, n_columns)