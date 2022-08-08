import os
import time
import requests
import numpy as np
import pandas as pd
from math import ceil
from itertools import product
from kauffman import constants as c
from webdriver_manager.chrome import ChromeDriverManager
from joblib import Parallel, delayed
from kauffman.tools._etl import state_msa_cross_walk as state_msa_cw
from kauffman.tools._etl import fips_state_cross_walk as fips_state_cw
from kauffman.tools._etl import load_CBSA_cw

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

"""
https://lehd.ces.census.gov/applications/help/led_extraction_tool.html#!qwi
"""

# WARNING: Please ensure that QWI has consistent releases for all 50 states
# before running. Here is a link to validate that folder containing most recent
# release has all 50 states + D.C.: https://lehd.ces.census.gov/data/qwi/.
# This link: https://lehd.ces.census.gov/doc/QWI_data_notices.pdf is updated
# by the Census Bureau whenever a complete new release of QWI data are 
# available.


def _get_year_groups(state_dict, max_years_per_call):
    years = list(range(
        int(state_dict['start_year']), 
        int(state_dict['end_year']) + 1
    ))
    if max_years_per_call == 1:
        return years
    else:
        n_bins = ceil(len(years) / min(max_years_per_call, len(years)))
        return [f'from{x[0]}to{x[-1]}' for x in np.array_split(years, n_bins)]
    

def _get_url_groups(
    obs_level, looped_strata, max_years_per_call, private, state_list, fips_list
):
    out_lst = []
    d = c.qwi_start_to_end_year()

    var_to_levels = {
        **c.qwi_strata_to_levels,
        **{'quarter':[x for x in range(1,5)]}
    }
    if private and 'industry' in looped_strata:
        var_to_levels['industry'] = [
            i for i in var_to_levels['industry'] if i != '92'
        ]

    if fips_list:
        region_years = [
            (state, region, year) 
            for state, region in fips_list
            for year in _get_year_groups(d[state], max_years_per_call)
        ]
    else:
        region_years = [
            (state, None, year) for state in state_list
            for year in _get_year_groups(d[state], max_years_per_call)
        ]
        if obs_level in ['county', 'msa']:
            missing_dict = c.qwi_missing_counties if obs_level == 'county' \
                else c.qwi_missing_msas
            region_years += [
                (state, ','.join(missing_dict[state]), year)
                for state in set(missing_dict) & set(state_list)
                for year in _get_year_groups(d[state], max_years_per_call)
            ]

    out_lst += [{
        **{
            'state_fips':value[0][0], 
            'region_fips':value[0][1], 
            'time':value[0][2]
        },
        **{
            f'{label}':value[i + 1]
            for i,label in enumerate(looped_strata) 
        }   
    } for value in product(
        region_years, 
        *[var_to_levels[s] for s in looped_strata]
    )]

    return out_lst


def _get_database_name(worker_char):
    if 'education' in worker_char:
        return 'se'
    elif 'race' in worker_char or 'ethnicity' in worker_char:
        return 'rh'
    else:
        return 'sa'


def _build_url(
        loop_var, non_loop_var, indicator_list, obs_level, private, census_key
    ):
    # API has started including all industry 3-digit subsectors and 4-digit 
    # groups in calls--howevever, it doesn't allow for filtering by ind_level
    # Including this code to only include the 2-digit level industries for now
    if 'industry' in non_loop_var:
        non_loop_var = [x for x in non_loop_var if x != 'industry']
        industries = c.qwi_strata_to_levels['industry']
        if private:
            industries = [x for x in industries if x != "92"]
        loop_var['industry'] = '&industry='.join(industries)
    
    base_url = 'https://api.census.gov/data/timeseries/qwi'
    database = _get_database_name(list(loop_var) + non_loop_var)
    get_statement = ','.join(indicator_list + non_loop_var + ['geo_level'])
    loop_section = f'&'.join([
        f'{k}={loop_var[k]}' 
        for k in loop_var
        if k != 'state_fips' and k != 'region_fips'
    ])
    ownercode = 'A05' if private == True else 'A00'
    state_fips, region_fips = loop_var['state_fips'], loop_var['region_fips']

    if obs_level == 'msa':
        for_region = 'for=metropolitan%20statistical%20area/' \
            + 'micropolitan%20statistical%20area:'
        if region_fips:
            for_region += f'{region_fips}&in=state:{state_fips}'
        else:
            for_region += f'*&in=state:{state_fips}'
    elif obs_level == 'county':
        if region_fips:
            if ',' not in region_fips:
                region_fips = region_fips[-3:]
            for_region = f'for=county:{region_fips}&in=state:{state_fips}'
        else:
            for_region = f'for=county:*&in=state:{state_fips}'
    else:
        for_region = f'for=state:{state_fips}'

    return f'{base_url}/{database}?get={get_statement}&{for_region}' \
        + f'&ownercode={ownercode}&{loop_section}&key={census_key}'


def _fetch_from_url(url, session):
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


def _led_scrape_data(private, firm_char, worker_char):
    pause1 = 1
    pause2 = 3

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("window-size=1920x1080")

    driver = webdriver.Chrome(
        ChromeDriverManager().install(),
        options=chrome_options
    )
    driver.get('https://ledextract.ces.census.gov/static/data.html')

    # Reset selected states
    # For some reason, the default right now is to select Wisconsin, so this
    # code is to reset that (by unchecking and then rechecking "all")
    # TODO: We need to come up with a way to handle changes to the default 
    # checkboxes...
    time.sleep(pause1)
    driver.find_element(By.XPATH, '//input[@name="areas_list_all"]').click()
    driver.find_element(By.XPATH, '//input[@name="areas_list_all"]').click()
    time.sleep(pause1)

    # Select US
    driver.find_element(
        By.XPATH, 
        '//input[@aria-label="National (50 States + DC) 00"]'
    ).click()
    time.sleep(pause1)
    driver.find_element(By.ID, 'continue_with_selection_label').click()

    # Firm Characteristics
    if not private:
        driver.find_element(By.XPATH, '//input[@data-label="All Ownership"]') \
            .click()

    if 'firmage' in firm_char:
        driver.find_element(
            By.XPATH, 
            '//*[text()="Firm Age, Private Ownership"]'
        ).click()
        driver.find_element(By.XPATH, '//input[@name="firmage_all"]').click()

    if 'firmsize' in firm_char:
        driver.find_element(
            By.XPATH, 
            '//*[text()="Firm Size, Private Ownership"]'
        ).click()
        driver.find_element(By.XPATH, '//input[@name="firmsize_all"]').click()

    if 'industry' in firm_char:
        driver.find_element(By.XPATH, '//input[@name="industries_list_all"]') \
            .click()

    driver.find_element(By.ID, 'continue_to_worker_char').click()

    # Worker Characteristics
    if set(worker_char) in [{'sex', 'agegrp'}, {'sex'}, {'agegrp'}]:
        if 'sex' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_sa_sex_all"]'
            ).click()
        if 'agegrp' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_sa_age_all"]'
            ).click()
    elif set(worker_char) in [{'sex', 'education'}, {'education'}]:
        driver.find_element(By.XPATH, '//*[text()="Sex and Education"]').click()
        if 'sex' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_se_sex_all"]'
            ).click()
        if 'education' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_se_education_all"]'
            ).click()
    else:
        driver.find_element(By.XPATH, '//*[text()="Race and Ethnicity"]') \
            .click()
        if 'race' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_rh_race_all"]'
            ).click()
        if 'ethnicity' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_rh_ethnicity_all"]'
            ).click()

    driver.find_element(By.ID, 'continue_to_indicators').click()

    # Indicators
    for _ in range(0, 3):
        driver.find_element(By.CLASS_NAME, 'ClosedGroup').click()
        time.sleep(pause2)
    for box in range(1,32):
        driver.find_elements(By.NAME, 'indicator')[box].click()
        # time.sleep(pause1)
    driver.find_element(By.ID, 'continue_to_quarters').click()

    # Quarters
    for quarter in range(1, 5):
        driver.find_element(
            By.XPATH, '//*[@title="Check All Q{}"]'.format(quarter)
        ).click()
    driver.find_element(By.ID, 'continue_to_export').click()

    # Summary and Export
    time.sleep(pause2)
    driver.find_element(By.ID, 'submit_request').click()

    try:
        element = WebDriverWait(driver, 60) \
            .until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element(By.LINK_TEXT, 'CSV').get_attribute('href')
        return pd.read_csv(href)


def _check_loop_group(group, target, winning_combo):
    product = 1
    for c in group.values():
        product *= c

    if product <= target:
        if product > winning_combo[1]:
            return ([k for k in group.keys()], product)
        else:
            return winning_combo

    for k in group.keys():
        combo_subset = group.copy()
        combo_subset.pop(k)
        winning_combo = _check_loop_group(combo_subset, target, winning_combo)
        
    return winning_combo


def _choose_loops(strata, obs_level, indicator_list):
    loopable_dict = {
        **{k:v for k,v in c.qwi_strata_to_nlevels.items() if k in strata},
        **{'quarter':4}
    }
    n_columns = len(
        strata + indicator_list \
        + ['geo_level', 'quarter', 'region', 'state', 'ownercode', 'time',
            'key']
    )
    target = (c.API_CELL_LIMIT/n_columns) \
        / c.qwi_geo_to_max_cardinality[obs_level]

    winning_combo = _check_loop_group(loopable_dict, target, (None, 0))
    loop_over_list = [
        l for l in loopable_dict.keys() 
        if l not in winning_combo[0]
    ]
    max_years_per_call = int(target/winning_combo[1])

    return loop_over_list, winning_combo[0], max_years_per_call


def _api_fetch_data(
    indicator_list, obs_level, firm_char, worker_char, private, key, n_threads,
    state_list=[], fips_list=[]
):
    looped_strata, non_loop_var, max_years_per_call = _choose_loops(
        firm_char + worker_char, obs_level, indicator_list
    )

    s = requests.Session()
    parallel = Parallel(n_jobs=n_threads, backend='threading')

    with parallel:
        df = pd.concat(
            parallel(
                delayed(_fetch_from_url)(
                    _build_url(
                        g, non_loop_var, indicator_list, obs_level, 
                        private, key), 
                    s
                )
                for g in _get_url_groups(
                    obs_level, looped_strata, max_years_per_call, private, 
                    state_list, fips_list
                )
            )
        )

    s.close()

    return df


def _cols_to_numeric(df, var_lst):
    df[var_lst] = df[var_lst].apply(pd.to_numeric, errors='ignore')
    return df


def _annualize_data(df, annualize, covars):
    if not annualize:
        return df
    elif annualize == 'March':
        df = df \
            .assign(
                quarter=lambda x: x['time'].str[-1:],
                time=lambda x: x.apply(
                    lambda y: int(y['time'][:4]) - 1 \
                        if y['quarter'] == '1' else int(y['time'][:4]),
                    axis=1
                )
            ) \
            .astype({'time': 'str'}) \
            .drop('quarter', 1)
    else:
        df = df \
            .assign(
                time=lambda x: x['time'].str[:4].astype(int),
            )
    return df \
        .assign(
            row_count=lambda x: x['fips'] \
                .groupby([x[var] for var in covars], dropna=False) \
                .transform('count')
        ) \
        .query('row_count == 4') \
        .drop(columns=['row_count']) \
        .groupby(covars) \
        .apply(lambda x: pd.DataFrame.sum(x.set_index(covars), skipna=False)) \
        .reset_index(drop=False)


def _create_fips(df, obs_level):
    if obs_level == 'state':
        df['fips'] = df['state'].astype(str)
    elif obs_level == 'county':
        df['fips'] = df['state'].astype(str) + df['county'].astype(str)
    elif obs_level == 'msa':
        df['fips'] = df \
            ['metropolitan statistical area/micropolitan statistical area'] \
            .astype(str)
    else:
        df = df.assign(fips='00')
    return df.assign(region=lambda x: x['fips'].map(c.all_fips_to_name))


def _filter_strata_totals(df, firm_char, worker_char, strata_totals):
    strata = firm_char + worker_char
    df = df.astype(dict(zip(strata, ['string'] * len(strata))))

    if not strata_totals and strata:
        strata_to_total_cat = {s:c.qwi_strata_to_levels[s][0] for s in strata}
        df = df.query(
            'and '.join([
                f'{stratum} != "{strata_to_total_cat[stratum]}"'
                for stratum in strata
            ])
        )

    return df


def _aggregate_msas(df, covars, obs_level):
    if obs_level != 'msa':
        return df
    return df \
        .assign(
            msa_states=lambda x: x[['state', 'fips']].groupby('fips') \
                .transform(lambda y: len(y.unique().tolist())),
            time_count=lambda x: x[covars + ['msa_states']].groupby(covars) \
                .transform('count')
        ) \
        .query('time_count == msa_states') \
        .drop(columns=['msa_states', 'time_count']) \
        .groupby(covars).sum() \
        .reset_index(drop=False)


def _state_overlap(x, state_list0):
    if list(set(x.split(', ')[1].split('-')) & set(state_list0)):
        return 1
    else:
        return 0


def _remove_extra_msas(df, state_list, state_list0):
    if sorted(state_list) == sorted(state_list0):
        return df
    else:
        return df \
            .assign(_keep=lambda x: x['region'] \
                .apply(lambda y: _state_overlap(
                    y, [c.state_fips_to_abb[fips] for fips in state_list0]
                ))) \
            .query('_keep == 1') \
            .drop('_keep', 1)


def _create_data(
    indicator_list, obs_level, state_list, fips_list, private, annualize, 
    firm_char, worker_char, strata_totals, key, n_threads, state_list0=None
):
    covars = ['time', 'fips', 'region', 'ownercode', 'geo_level'] \
        + firm_char + worker_char

    state_list0 = state_list
    if (len(state_list) < 51) and (obs_level == 'msa') and not fips_list:
        state_list = state_msa_cw(state_list, 'all') \
            ['fips_state'].unique().tolist()

    if obs_level == 'us':
        df = _led_scrape_data(private, firm_char, worker_char) \
            .assign(
                time=lambda x: x['year'].astype(str) + '-Q' \
                    + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan
            ) \
            .rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})
    else:
        if fips_list and obs_level == 'msa':
            df = _api_fetch_data(
                indicator_list, obs_level, firm_char, worker_char, private, key,
                n_threads, fips_list=[
                    tuple(row) 
                    for row in fips_state_cw(fips_list, obs_level).values
                ]
            )
        elif fips_list and obs_level == 'county':
            df = _api_fetch_data(
                indicator_list, obs_level, firm_char, worker_char, private, key,
                n_threads, fips_list=list(
                    zip([x[:2] for x in fips_list], fips_list)
                )
            )
        else:
            df = _api_fetch_data(
                indicator_list, obs_level, firm_char, worker_char, private, key,
                n_threads, state_list=state_list
            )

    df.drop_duplicates(inplace=True)

    return df \
        .pipe(_create_fips, obs_level) \
        .pipe(_cols_to_numeric, indicator_list) \
        .pipe(_filter_strata_totals, firm_char, worker_char, strata_totals) \
        .pipe(_aggregate_msas, covars, obs_level) \
        .pipe(_remove_extra_msas, state_list, state_list0) \
        [covars + indicator_list] \
        .pipe(_annualize_data, annualize, covars) \
        .sort_values(covars) \
        .reset_index(drop=True)


def _estimate_data_shape(
    indicator_list, obs_level_lst, firm_char, worker_char, strata_totals, 
    state_list, fips_list
):
    n_columns = len(
        indicator_list + firm_char + worker_char \
        + ['time', 'fips', 'region', 'ownercode', 'geo_level']
    )
    row_estimate = 0

    for level in obs_level_lst:
        if level == 'us':
            year_regions = 28
        elif level == 'state':
            year_regions = pd.DataFrame(c.qwi_start_to_end_year()) \
                .T.reset_index() \
                .rename(columns={'index':'state'}) \
                .astype({'start_year':'int', 'end_year':'int'}) \
                .query(f'state in {state_list}') \
                .assign(n_years=lambda x: x['end_year'] - x['start_year'] + 1) \
                ['n_years'].sum()
        elif fips_list or level in ['msa', 'county']:
            d = c.qwi_start_to_end_year()
            query = f"fips_{level} in {fips_list}" if fips_list \
                else f"fips_state in {state_list}"
            year_regions = load_CBSA_cw() \
                .query(query) \
                [[f'fips_{level}', 'fips_state']] \
                .drop_duplicates() \
                .groupby('fips_state').count() \
                .reset_index() \
                .assign(
                    n_years=lambda x: x['fips_state'] \
                        .map(
                            lambda y: int(d[y]['end_year']) \
                            - int(d[y]['start_year']) + 1
                        ),
                    year_regions=lambda x: x['n_years']*x[f'fips_{level}']
                ) \
                ['year_regions'].sum()

        # Get n_strata_levels
        strata_levels = 1
        strata = worker_char + firm_char
        strata_to_nlevels = c.qwi_strata_to_nlevels
        if not strata_totals:
            strata_to_nlevels = {k:v - 1 for k,v in strata_to_nlevels.items()}

        for s in strata:
            strata_levels *= strata_to_nlevels[s]
        
        row_estimate += year_regions*strata_levels*4

    return (row_estimate, n_columns)


def qwi(
    indicator_list='all', obs_level='all', state_list='all', fips_list=[],
    private=False, annualize='January', firm_char=[], worker_char=[], 
    strata_totals=False, key=os.getenv("CENSUS_KEY"), n_threads=1
):
    """
    Fetches nation-, state-, MSA-, or county-level Quarterly Workforce 
    Indicators (QWI) data either from the LED extractor tool in the case of 
    national data (https://ledextract.ces.census.gov/static/data.html) or from 
    the Census's API in the case of state, MSA, or county 
    (https://api.census.gov/data/timeseries/qwi/sa/examples.html).
    FTP: https://lehd.ces.census.gov/data/qwi/R2021Q1/us/

    obs_level: str
        'state': resident population of state from 1990 through 2019
        'msa': resident population of msa from 1990 through 2019
        'county': resident population of county from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019
        'all': default, returns data on all of the above observation levels

    indicator_list: str, lst
        'all': default, will return all QWI indicators
        otherwise: return list of indicators plus 'time', 'ownercode', 
            'firmage', and 'fips'

        EarnBeg: End-of-Quarter Employment: Average Monthly Earnings
        EarnHirAS: Hires All (Stable): Average Monthly Earnings
        EarnHirNS: Hires New (Stable): Average Monthly Earnings
        EarnS: Full Quarter Employment (Stable): Average Monthly Earnings
        EarnSepS: Separations (Stable): Average Monthly Earnings
        Emp: Beginning-of-Quarter Employment: Counts
        EmpEnd: End-of-Quarter Employment: Counts
        EmpS: Full-Quarter Employment (Stable): Counts
        EmpSpv: Full-Quarter Employment in the Previous Quarter: Counts
        EmpTotal: Employment-Reference Quarter: Counts
        FrmJbC: Firm Job Change:Net Change
        FrmJbCS: Job Change (Stable): Net Change
        FrmJbGn: Firm Job Gains: Counts (Job Creation)
        FrmJbGnS: Firm Job Gains (Stable): Counts
        FrmJbLs: Firm Job Loss: Counts (Job Destruction)
        FrmJbLsS: Firm Job Loss (Stable): Counts
        HirA: Hires All: Counts (Accessions)
        HirAEnd: End-of-Quarter Hires
        HirAEndR: End-of-Quarter Hiring Rate
        HirAEndRepl: Replacement Hires
        HirAEndReplr: Replacement Hiring Rate
        HirAs: Hires All (Stable): Counts (Flows into Full-QuarterEmployment)
        HirN: Hires New: Counts
        HirNs: Hires New (Stable): Counts (New Hires to Full-Quarter Status)
        HirR: Hires Recalls: Counts
        Payroll: Total Quarterly Payroll: Sum
        Sep: Separations: Counts
        SepBeg: Beginning-of-Quarter Separations
        SepBegR: Beginning-of-Quarter Separation Rate
        SepS: Separations (Stable): Counts (Flow out of Full-Quarter Employment)
        SepSnx: Separations (Stable), Next Quarter: Counts 
            (Flow out of Full-Quarter Employment)
        TurnOvrS: Turnover (Stable)

        ***HirAEndRepl  HirAEndReplr are not available for US

    state_list: str, lst
        'all': default, includes all US states and D.C.
        otherwise: a state or list of states, identified using postal code 
            abbreviations

    fips_list: lst
        msa or county fips to pull

    private: bool
        True: All private only
        False: All
        if by_age_size is not None, then private is set to True

    annualize: None, str
        'None': leave as quarterly data
        'January': annualize using Q1 as beginning of year
        'March': annualize using Q2 as beginning of year

    firm_char: lst, str
        empty: default
        'firmage': stratify by firm age
        'firmsize': stratify by firm size
        'industry': stratify by firm industry, NAICS 2-digit

    worker_char: lst, str
        'sex': stratify by worker sex
        'agegrp': stratify by worker age

        'sex': stratify by worker sex
        'education': stratify by worker education

        'race': worker race
        'ethnicity': worker ethnicity

    key: str
        Your Census Data API Key.
    """

    if obs_level in ['us', 'state', 'county', 'msa']:
        obs_level_lst = [obs_level]
    elif obs_level == 'all':
        obs_level_lst = ['us', 'state', 'county', 'msa']
    else:
        raise Exception('Invalid input to obs_level.')


    if state_list == 'all':
        state_list = [c.state_abb_to_fips[s] for s in c.states]
    else:
        state_list = [c.state_abb_to_fips[s] for s in state_list]

    if indicator_list == 'all':
        indicator_list = c.qwi_outcomes
    elif type(indicator_list) == str:
        indicator_list = [indicator_list]

    # todo: keep this?
    # if annualize and any(x in c.qwi_averaged_outcomes for x in indicator_lst):
    #     raise Exception('indicator_lst not compatible with annualize==True')


    firm_char = [firm_char] if type(firm_char) == str else firm_char
    if any(x in ['firmage', 'firmsize'] for x in firm_char):
        private = True
        print(
            'Warning: Firmage, firmsize only available when private = True.',
            'Variable private has been set to True.'
        )
    if obs_level in ['us', 'all'] and private == False:
        private = True
        print(
            'Warning: US-level data is only available when private=True.',
            'Variable "private" has been set to True.'
        )

    worker_char = [worker_char] if type(worker_char) == str else worker_char

    if set(worker_char) not in c.qwi_worker_crosses:
        raise Exception(
            'Invalid input to worker_char. See function documentation for' 
            'valid groups.'
        )

    if 'firmage' in firm_char and 'firmsize' in firm_char:
        raise Exception(
            'Invalid input to firm_char. Can only specify one of firmage or' \
            'firmsize.'
        )

    strata_totals = False if not (firm_char or worker_char) else strata_totals

    if fips_list and any([l not in ['msa', 'county'] for l in obs_level_lst]):
        raise Exception(
            'If fips_list is provided, obs_level must be either msa or county.'
        )

    estimated_shape = _estimate_data_shape(
        indicator_list, obs_level_lst, firm_char, worker_char, strata_totals, 
        state_list, fips_list
    )
    if estimated_shape[0] * estimated_shape[1] > 100000000:
        print(
            'Warning: You are attempting to fetch a dataframe of estimated',
            f'shape {estimated_shape}. You may experience memory errors.'
        )

    print('QWI Dynamic API version')

    return pd.concat(
            [
                _create_data(
                    indicator_list, level, state_list, fips_list, private, 
                    annualize, firm_char, worker_char, strata_totals, key, 
                    n_threads
                )
                for level in obs_level_lst
            ],
            axis=0
        )