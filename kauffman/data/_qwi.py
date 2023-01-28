import os
import time
import numpy as np
import pandas as pd
from math import ceil
from itertools import product
from kauffman import constants as c
from kauffman.tools import qwi_tools as q
from kauffman.tools import general_tools as g
from kauffman.tools import api_tools as api
from webdriver_manager.chrome import ChromeDriverManager

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def _year_groups(state_dict, max_years_per_call):
    years = list(range(state_dict['start_year'], state_dict['end_year'] + 1))
    if max_years_per_call == 1:
        return years
    else:
        n_bins = ceil(len(years) / max_years_per_call)
        return [f'from{x[0]}to{x[-1]}' for x in np.array_split(years, n_bins)]
    

def _url_groups(
    obs_level, looped_strata, max_years_per_call, private, state_list, 
    fips_list, annualize
):
    out_lst = []
    state_to_years = q._get_state_to_years(annualize)

    var_to_levels = {
        **c.QWI_STRATA_TO_LEVELS,
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
            for year in _year_groups(state_to_years[state], max_years_per_call)
        ]
    else:
        region_years = [
            (state, state if obs_level == 'state' else '*', year)
            for state in state_list
            for year in _year_groups(state_to_years[state], max_years_per_call)
        ]
        if obs_level in ['county', 'msa']:
            missing_dict = c.QWI_MISSING_COUNTIES if obs_level == 'county' \
                else c.QWI_MISSING_MSAS
            region_years += [
                (state, ','.join(missing_dict[state]), year)
                for state in set(missing_dict) & set(state_list)
                for year in _year_groups(state_to_years[state], max_years_per_call)
            ]

    out_lst += [{
        **{
            'state_fips':group[0][0], 
            'fips':group[0][1], 
            'time':group[0][2]
        },
        **{
            f'{strata}':group[i + 1]
            for i,strata in enumerate(looped_strata) 
        }
    } for group in product(
        region_years, 
        *[var_to_levels[s] for s in looped_strata]
    )]

    return out_lst


def _database_name(worker_char):
    if 'education' in worker_char:
        return 'se'
    elif 'race' in worker_char or 'ethnicity' in worker_char:
        return 'rh'
    else:
        return 'sa'


def _qwi_url(loop_var, non_loop_var, indicator_list, obs_level, private, key):
    # API has started including all industry 3-digit subsectors and 4-digit 
    # groups in calls--howevever, it doesn't allow for filtering by ind_level
    # Including this code to only include the 2-digit level industries for now
    if 'industry' in non_loop_var:
        non_loop_var = [x for x in non_loop_var if x != 'industry']
        industries = c.QWI_STRATA_TO_LEVELS['industry']
        if private:
            industries = [x for x in industries if x != "92"]
        loop_var['industry'] = '&industry='.join(industries)
    
    base_url = 'https://api.census.gov/data/timeseries/qwi'
    database = _database_name(list(loop_var) + non_loop_var)
    get_statement = ','.join(indicator_list + non_loop_var + ['geo_level'])
    loop_section = f'&'.join([
        f'{k}={loop_var[k]}' 
        for k in loop_var
        if k != 'state_fips' and k != 'fips'
    ])
    ownercode = 'A05' if private == True else 'A00'
    state_fips, fips = loop_var['state_fips'], loop_var['fips']

    in_state = True if obs_level != 'state' else False
    if obs_level == 'county' and fips != '*' and ',' not in fips:
        fips = fips[-3:]
    fips_section = api._fips_section(obs_level, fips, state_fips, in_state)

    key_section = f'&key={key}' if key else ''

    return f'{base_url}/{database}?get={get_statement}&for={fips_section}' \
        + f'&ownercode={ownercode}&{loop_section}{key_section}'


def _scrape_led_data(private, firm_char, worker_char):
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

    time.sleep(pause1)
    driver.find_element(By.LINK_TEXT, 'All QWI Measures').click()
    time.sleep(pause1)

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
    driver.find_element(By.XPATH, '//*[text()="Continue to Firm Characteristics"]').click()

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

    driver.find_element(By.XPATH, '//*[text()="Continue to Worker Characteristics"]').click()

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

    driver.find_element(By.XPATH, '//*[text()="Continue to Indicators"]').click()

    # Indicators
    driver.find_element(By.NAME, 'indicators_all').click()
    driver.find_element(By.XPATH, f'//*[text()="Employment Change, Individual"]').click()
    driver.find_element(By.XPATH, '//*[@data-part="indicators_employment_change_individual"]//*[@name="indicators_all"]').click()
    driver.find_element(By.XPATH, f'//*[text()="Employment Change, Firm"]').click()
    driver.find_element(By.XPATH, '//*[@data-part="indicators_employment_change_firm"]//*[@name="indicators_all"]').click()
    driver.find_element(By.XPATH, f'//*[text()="Earnings"]').click()
    driver.find_element(By.XPATH, '//*[@data-part="indicators_earnings"]//*[@name="indicators_all"]').click()

    driver.find_element(By.XPATH, '//*[text()="Continue to Quarters"]').click()

    # Quarters
    for quarter in range(1, 5):
        driver.find_element(
            By.XPATH, '//*[@title="Check All Q{}"]'.format(quarter)
        ).click()
    driver.find_element(By.XPATH, '//*[text()="Continue to Summary and Export"]').click()

    # Summary and Export
    time.sleep(pause2)
    driver.find_element(By.XPATH, '//*[text()="Submit Request"]').click()

    try:
        element = WebDriverWait(driver, 60) \
            .until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element(By.LINK_TEXT, 'CSV').get_attribute('href')
        return pd.read_csv(href)


def _optimal_loops(group, target, winning_combo):
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
        winning_combo = _optimal_loops(combo_subset, target, winning_combo)
        
    return winning_combo


def _loops_info(strata, obs_level, indicator_list):
    loopable_dict = {
        **{k:v for k,v in c.QWI_STRATA_TO_NLEVELS.items() if k in strata},
        **{'quarter':4}
    }
    n_columns = len(
        strata + indicator_list \
        + ['geo_level', 'quarter', 'region', 'state', 'ownercode', 'time',
            'key']
    )
    target = (c.API_CELL_LIMIT/n_columns) \
        / c.QWI_GEO_TO_MAX_CARDINALITY[obs_level]

    winning_combo = _optimal_loops(loopable_dict, target, (None, 0))
    loop_over_list = [
        l for l in loopable_dict.keys() 
        if l not in winning_combo[0]
    ]
    max_years_per_call = int(target/winning_combo[1])

    return loop_over_list, winning_combo[0], max_years_per_call


def _qwi_fetch_api_data(
    loop_var, non_loop_var, indicator_list, obs_level, private, key, s
):
    url = _qwi_url(
        loop_var, non_loop_var, indicator_list, obs_level, private, key
    )
    return api.fetch_from_url(url, s)


def _cols_to_numeric(df, var_lst):
    df[var_lst] = df[var_lst].apply(pd.to_numeric, downcast='integer')
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
        .groupby(covars).sum(min_count=4) \
        .reset_index(drop=False)


def _filter_strata_totals(df, firm_char, worker_char, strata_totals):
    strata = firm_char + worker_char
    df = df.astype(dict(zip(strata, ['string'] * len(strata))))

    if not strata_totals and strata:
        strata_to_total_cat = {s:c.QWI_STRATA_TO_LEVELS[s][0] for s in strata}
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
                .transform('nunique'),
            msa_row_count=lambda x: x[covars + ['msa_states']].groupby(covars) \
                .transform('count')
        ) \
        .query('msa_row_count == msa_states') \
        .drop(columns=['msa_states', 'msa_row_count']) \
        .groupby(covars).sum() \
        .reset_index(drop=False)


def _state_overlap(x, state_list_orig):
    if list(set(x.split(', ')[1].split('-')) & set(state_list_orig)):
        return 1
    else:
        return 0


def _remove_extra_msas(df, state_list, state_list_orig):
    if sorted(state_list) == sorted(state_list_orig):
        return df
    else:
        return df \
            .assign(_keep=lambda x: x['region'] \
                .apply(lambda y: _state_overlap(
                    y, [c.STATE_FIPS_TO_ABB[fips] for fips in state_list_orig]
                ))) \
            .query('_keep == 1') \
            .drop(columns='_keep')


def _qwi_fetch_data(
    indicator_list, obs_level, state_list, fips_list, private, annualize, 
    firm_char, worker_char, key, n_threads
):
    if obs_level == 'us':
        return _scrape_led_data(private, firm_char, worker_char) \
            .assign(
                time=lambda x: x['year'].astype(str) + '-Q' \
                    + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan
            ) \
            .rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})

    looped_strata, non_loop_var, max_years_per_call = _loops_info(
        firm_char + worker_char, obs_level, indicator_list
    )
    if fips_list:
        if obs_level == 'county':
            fips_list = list(zip([x[:2] for x in fips_list], fips_list))
        else:
            fips_list = [
                tuple(row)
                for row in g.geolevel_crosswalk('msa', 'state', fips_list) \
                    [['fips_state', 'fips_msa']].values
            ]
    groups = _url_groups(
        obs_level, looped_strata, max_years_per_call, private, state_list, 
        fips_list, annualize
    )

    return api.run_in_parallel(
        data_fetch_fn = _qwi_fetch_api_data, 
        groups = groups,
        constant_inputs = [
            non_loop_var, indicator_list, obs_level, private, key
        ],
        n_threads=n_threads
    )


def qwi(
    indicator_list='all', obs_level='us', state_list='all', fips_list=[],
    private=False, annualize='January', firm_char=[], worker_char=[], 
    strata_totals=False, enforce_release_consistency=False, 
    key=os.getenv("CENSUS_KEY"), n_threads=1
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

    if enforce_release_consistency:
        q.consistent_releases(enforce=True)

    state_list = c.STATES if state_list == 'all' else state_list
    state_list = [c.STATE_ABB_TO_FIPS[s] for s in state_list]

    if indicator_list == 'all':
        indicator_list = c.QWI_OUTCOMES
    elif type(indicator_list) == str:
        indicator_list = [indicator_list]

    # todo: keep this?
    # if annualize and any(x in c.qwi_averaged_outcomes for x in indicator_lst):
    #     raise Exception('indicator_list not compatible with annualize==True')

    firm_char, worker_char = g.as_list(firm_char), g.as_list(worker_char)

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

    if set(worker_char) not in c.QWI_WORKER_CROSSES:
        raise Exception(
            'Invalid input to worker_char. See function documentation for' 
            'valid groups.'
        )

    if 'firmage' in firm_char and 'firmsize' in firm_char:
        raise Exception(
            'Invalid input to firm_char. Can only specify one of firmage or'
            'firmsize.'
        )

    strata_totals = False if not (firm_char or worker_char) else strata_totals

    if fips_list and obs_level not in ['county', 'msa']:
        raise Exception(
            'If fips_list is provided, obs_level must be either msa or county.'
        )

    estimated_shape = q.estimate_data_shape(
        indicator_list, obs_level, firm_char, worker_char, strata_totals, 
        state_list, fips_list
    )
    if estimated_shape[0] * estimated_shape[1] > 100000000:
        print(
            'Warning: You are attempting to fetch a dataframe of estimated',
            f'shape {estimated_shape}. You may experience memory errors.'
        )

    # Warn users if they didn't provide a key
    if key == None:
        print('WARNING: You did not provide a key. Too many requests will ' \
            'result in an error.')

    covars = ['time', 'fips', 'region', 'ownercode', 'geo_level'] \
        + firm_char + worker_char

    state_list_orig = state_list
    if (len(state_list) < 51) and (obs_level == 'msa') and not fips_list:
        state_list = g.geolevel_crosswalk(
                from_geo='state', to_geo='msa', 
                from_fips_list=state_list, msa_coidentify_state=True
            ) \
            ['fips_state'].unique().tolist()

    return _qwi_fetch_data(
            indicator_list, obs_level, state_list, fips_list, private, 
            annualize, firm_char, worker_char, key, n_threads
        ) \
        .drop_duplicates() \
        .pipe(api._create_fips, obs_level) \
        .pipe(_cols_to_numeric, indicator_list) \
        .pipe(_filter_strata_totals, firm_char, worker_char, strata_totals) \
        .pipe(_aggregate_msas, covars, obs_level) \
        .pipe(_remove_extra_msas, state_list, state_list_orig) \
        [covars + indicator_list] \
        .pipe(_annualize_data, annualize, covars) \
        .sort_values(covars) \
        .reset_index(drop=True)