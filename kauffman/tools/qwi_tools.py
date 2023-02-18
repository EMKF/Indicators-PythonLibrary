import requests
import pandas as pd
from joblib import Parallel, delayed
from kauffman import constants as c
from kauffman.data_fetch._qwi import _qwi_constants as qc
from kauffman.tools.general_tools import CBSA_crosswalk


def _get_state_release_info(state, session):
    """Get the QWI latest release information for a given state."""
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


def latest_releases(state_list, n_threads=30):
    """
    Get the latest release of QWI data by state.

    Parameters
    ----------
    state_list: list
        The list of states to get release info for
    n_threads: int, default 30
        Number of threads to use in parallelization of the code

    Returns
    -------
    DataFrame
        The release info
    """
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
    """
    Check whether the QWI data on the Census API comes from consistent releases 
    at the moment.

    Parameters
    ----------
    state_list: str, default 'all'
        List of states to check across
    n_threads: int, default 30
        Number of threads to use in parallelization of the code
    enforce: bool, default False
        Whether to throw an error if there are not consistent releases

    Returns
    -------
    Bool
        Whether there are consistent releases across all states in state_list
    """
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


def _get_state_to_years(annualize=False):
    """
    Fetch the dictionary mapping state to the years (and quarters, optionally)
    that the state's QWI data is available for.

    Parameters
    ----------
    annualize: {'January', 'April', False}
        The annualization method, if any. If 'January', Q1 is considered the
        start of the year; if 'April', Q2 is considered the start of the year.
        If False, get both the year and quarter of availability.

    Returns
    -------
    Dict
        Mapping of state to QWI availability.
    """
    df = pd.read_html('https://ledextract.ces.census.gov/loading_status.html') \
        [0][['State', 'Start Quarter', 'End Quarter']] \
        .assign(
            start_quarter=lambda x: x['Start Quarter'].str[-1:].astype(int),
            end_quarter=lambda x: x['End Quarter'].str[-1:].astype(int),
            start_year=lambda x: x['Start Quarter'].str.split().str[0] \
                .astype(int),
            end_year=lambda x: x['End Quarter'].str.split().str[0].astype(int),
            fips=lambda x: x['State'].map(c.STATE_ABB_TO_FIPS)
        )
    if annualize:
        start_of_year, end_of_year = (1,4) if annualize == 'January' else (2,1)
        df = df.assign(
            start_year=lambda x: x.start_year \
                .where(x.start_quarter <= start_of_year, x.start_year + 1),
            end_year=lambda x: x.end_year \
                .where(x.end_quarter >= end_of_year, x.end_year - 1)
        )
    return df \
        .set_index('fips') \
        [['start_year', 'end_year']] \
        .to_dict('index')


def estimate_data_shape(
    indicator_list, geo_level, firm_char, worker_char, strata_totals, 
    state_list, fips_list
):
    """Given qwi function inputs, estimate shape of raw data."""
    n_columns = len(
        indicator_list + firm_char + worker_char \
        + ['time', 'fips', 'region', 'ownercode']
    )
    row_estimate = 0
    state_to_years = _get_state_to_years(False)

    if geo_level == 'us':
        year_regions = state_to_years['00']['end_year'] \
            - state_to_years['00']['start_year'] + 1
    elif geo_level == 'state':
        year_regions = pd.DataFrame(state_to_years) \
            .T.reset_index() \
            .rename(columns={'index':'state'}) \
            .query(f'state in {state_list}') \
            .assign(n_years=lambda x: x['end_year'] - x['start_year'] + 1) \
            ['n_years'].sum()
    else:
        query = f"fips_{geo_level} in {fips_list}" if fips_list \
            else f"fips_state in {state_list}"
        year_regions = CBSA_crosswalk() \
            .query(query) \
            [[f'fips_{geo_level}', 'fips_state']] \
            .drop_duplicates() \
            .groupby('fips_state').count() \
            .reset_index() \
            .assign(
                n_years=lambda x: x['fips_state'] \
                    .map(
                        lambda state: state_to_years[state]['end_year'] \
                            - state_to_years[state]['start_year'] + 1
                    ),
                year_regions=lambda x: x['n_years']*x[f'fips_{geo_level}']
            ) \
            ['year_regions'].sum()

    # Get n_strata_levels
    strata_levels = 1
    strata = worker_char + firm_char
    strata_to_nlevels = qc.STRATA_TO_NLEVELS
    if not strata_totals:
        strata_to_nlevels = {k:v - 1 for k,v in strata_to_nlevels.items()}
    for s in strata:
        strata_levels *= strata_to_nlevels[s]
    
    row_estimate += year_regions*strata_levels*4

    return (row_estimate, n_columns)


def _map_state_to_years(state, d):
    return list(range(d[state]['start_year'], d[state]['end_year'] + 1))


def missing_obs(df, geo_level, state_list, fips_list, worker_char, firm_char, 
annualize, strata_totals):
    """
    Finds the observations that are expected in the QWI data but not present.

    Parameters
    ----------
    df: DataFrame
        The QWI data
    geo_level: {'county', 'msa', 'state', 'us'}
        The geographical level of the data
    worker_char: list
        The worker characteristics the data is stratified by
    firm_char: list
        The firm characteristics the data is stratified by

    Returns
    -------
    DataFrame
        The index of the missing observations
    """
    if geo_level == 'us':
        expected_index = pd.DataFrame({'time':list(range(1990, 2021))}) \
            .assign(fips='01')
    else:
        state_to_years = _get_state_to_years(annualize)
        state_list = c.STATES if state_list == 'all' else state_list
        state_list = [c.STATE_ABB_TO_FIPS[s] for s in state_list]
        fips_cols = list({'fips_state', f'fips_{geo_level}'})

        expected_index = CBSA_crosswalk() \
            [fips_cols] \
            .query(f'fips_state in {state_list}')
        if fips_list:
            expected_index = expected_index \
                .query(f'fips_{geo_level} in {fips_list}')
            
        expected_index = expected_index \
            .drop_duplicates() \
            .assign(
                time=lambda x: x.fips_state.apply(
                    _map_state_to_years, 
                    args=[state_to_years]
                )
            ) \
            .explode('time')

        strata = worker_char + firm_char
        strata_dict = qc.STRATA_TO_LEVELS if strata_totals else {
            k:v[1:] for k,v in qc.STRATA_TO_LEVELS.items()
        }
        for s in strata:
            expected_index[s] = [strata_dict[s]] * len(expected_index)
            expected_index = expected_index.explode(s)

        expected_index = expected_index \
            .assign(fips=lambda x:x[f'fips_{geo_level}']) \
            .drop(columns=fips_cols)

    if not annualize:
        expected_index = expected_index.assign(
                quarter=[[1,2,3,4]]*len(expected_index)
            ) \
            .explode('quarter')

    var_to_dtypes = {**{'time':int, 'fips':str}, **{var:str for var in strata}}
    expected_index = expected_index \
        .astype(var_to_dtypes) \
        .reset_index(drop=True)

    return df[[c for c in expected_index.columns]] \
            .merge(expected_index, how='right', indicator=True) \
            .query('_merge != "both"') \
            .drop(columns='_merge') \
            .reset_index(drop=True)