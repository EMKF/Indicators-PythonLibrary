import os
import sys
import time
import requests
import numpy as np
import pandas as pd
from itertools import product
from kauffman import constants as c
from webdriver_manager.chrome import ChromeDriverManager
from joblib import Parallel, delayed
from kauffman.tools._etl import state_msa_cross_walk, fips_state_cross_walk, load_CBSA_cw

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

"""
https://lehd.ces.census.gov/applications/help/led_extraction_tool.html#!qwi
"""


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _url_groups(firm_char, private, state_lst, fips_lst):
    out_lst = []
    d = c.qwi_start_to_end_year()

    firmages = [x for x in range(0, 6)] if 'firmage' in firm_char else [0]
    firmsizes = [x for x in range(0, 6)] if 'firmsize' in firm_char else [0]

    if 'industry' in firm_char:
        industries = ['00', '11', '21', '22', '23', '31-33', '42', '44-45', '51', '52', '53', '54', '55', '56', '61', '62', '71', '72', '81', '92']
        if private:
            industries.remove('92')
    else:
        industries = ['00']

    if fips_lst:
        for fips in fips_lst:
            state = fips[0]
            region = fips[1]

            start_year = int(d[state]['start_year'])
            end_year = int(d[state]['end_year'])
            years = [x for x in range(start_year, end_year + 1)]
            out_lst += list(product([state], years, firmages, firmsizes, industries, [region]))
    else:
        for state in state_lst:
            start_year = int(d[state]['start_year'])
            end_year = int(d[state]['end_year'])
            years = [x for x in range(start_year, end_year + 1)]
            out_lst += list(product([state], years, firmages, firmsizes, industries, [None]))
    return out_lst


def database_name(worker_char):
    if 'education' in worker_char:
        return 'se'
    elif 'race' in worker_char or 'ethnicity' in worker_char:
        return 'rh'
    else:
        return 'sa'


def _build_url(fips, year, firmage, firmsize, industry, region_fips, indicator_lst, region, worker_char, private, census_key):
    base_url = 'https://api.census.gov/data/timeseries/qwi/'
    var_lst = ','.join(indicator_lst + worker_char)
    firm_char_section = f'firmsize={firmsize}&firmage={firmage}&industry={industry}'
    private = 'A05' if private == True else 'A00'
    database = database_name(worker_char)

    if region == 'msa':
        if region_fips:
            for_region = f'for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{region_fips}&in=state:{fips}'
        else:
            for_region = f'for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&in=state:{fips}'
    elif region == 'county':
        if region_fips:
            for_region = f'for=county:{region_fips[-3:]}&in=state:{fips}'
        else:
            for_region = f'for=county:*&in=state:{fips}'
    else:
        for_region = f'for=state:{fips}'
    return '{0}{1}?get={2}&{3}&time={4}&ownercode={5}&{6}&key={7}'. \
        format(base_url, database, var_lst, for_region, year, private, firm_char_section, census_key)


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
                print('Blank url:', url)
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
    return df


def _qwi_ui_fetch_data(private, firm_char, worker_char, region='us'):
    pause1 = 1
    pause2 = 3

    chrome_options = Options()
    chrome_options.add_argument('--headless')

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get('https://ledextract.ces.census.gov/static/data.html')

    # Geography
    if region == 'LA':
        driver.find_elements(By.LINK_TEXT, 'California')[0].click()
        time.sleep(pause2)
        driver.find_element(By.XPATH, '//*[@title="Click to select Micro/Metropolitan Areas"]').click()
        time.sleep(pause2)
        driver.find_element(By.XPATH, '//*[@title="Click to select 06 California"]').click()
        driver.find_element(By.XPATH, '//*[@title="Click to select 0631080 Los Angeles-Long Beach-Anaheim, CA"]').click()
    time.sleep(pause1)
    driver.find_element(By.ID, 'continue_with_selection_label').click()

    # Firm Characteristics
    if private:
        driver.find_element(By.XPATH, '//*[@id="dijit_layout_ContentPane_8"]/form[2]/div[3]/label/input').click()

    if 'firmage' in firm_char:
        for box in range(1, 6):
            driver.find_element(By.ID, 'dijit_form_CheckBox_{}'.format(box)).click()

    if 'firmsize' in firm_char:
        for box in range(7, 12):
            driver.find_element(By.ID, 'dijit_form_CheckBox_{}'.format(box)).click()

    if 'industry' in firm_char:
        elems = driver.find_elements(By.XPATH, "//a[@href]")[12]
        driver.execute_script("arguments[0].click();", elems)
    driver.find_element(By.ID, 'continue_to_worker_char').click()

    # Worker Characteristics
    if 'sex' in worker_char:
        driver.find_element(By.ID, 'dijit_form_CheckBox_13').click()
        driver.find_element(By.ID, 'dijit_form_CheckBox_14').click()
    if 'agegrp' in worker_char:
        for box in range(1, 9):
            driver.find_element(By.XPATH, f'//input[@value="A0{box}"]').click()
    if 'education' in worker_char:
        driver.find_element(By.ID, 'worker_char_switch').click()
        driver.find_element(By.ID, 'dijit_MenuItem_2').click()
        for box in range(1, 6):
            driver.find_element(By.XPATH, f'//input[@value="E{box}"]').click()
    if 'race' in worker_char:
        driver.find_element(By.ID, 'worker_char_switch').click()
        driver.find_element(By.ID, 'dijit_MenuItem_3').click()
        for box in range(1, 8):
            for element in driver.find_elements(By.XPATH, f'//input[@value="A{box}"]'):
                element.click()
    driver.find_element(By.ID, 'continue_to_indicators').click()

    # Indicators
    for _ in range(0, 3):
        driver.find_element(By.CLASS_NAME, 'ClosedGroup').click() # Scroll through all the options
        time.sleep(pause2)
    for box in driver.find_elements(By.NAME, 'indicator'):
        if box.get_attribute('value') != "001_Emp": box.click()
        # time.sleep(pause1)
    driver.find_element(By.ID, 'continue_to_quarters').click()

    # Quarters
    for quarter in range(1, 5):
        driver.find_element(By.XPATH, '//*[@title="Check All Q{}"]'.format(quarter)).click()
    driver.find_element(By.ID, 'continue_to_export').click()

    # Summary and Export
    time.sleep(pause2)
    driver.find_element(By.ID, 'submit_request').click()

    try:
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element(By.LINK_TEXT, 'CSV').get_attribute('href')
        return pd.read_csv(href)


def _us_fetch_data(private, firm_char, worker_char):
    return _qwi_ui_fetch_data(private, firm_char, worker_char)


def _LA_fetch_data(firm_char, worker_char):
    df = _qwi_ui_fetch_data(True, firm_char, worker_char, region='LA'). \
            assign(
                time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan,
                state='06'
            ). \
            rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})
    df['metropolitan statistical area/micropolitan statistical area'] = '31080'
    return df


def _county_msa_state_fetch_data(indicator_lst, obs_level, firm_char, worker_char, private, key, n_threads, state_lst=[], fips_lst=[]):
    s = requests.Session()
    parallel = Parallel(n_jobs=n_threads, backend='threading')

    with parallel:
        df = pd.concat(
            parallel(
                delayed(_fetch_from_url)(_build_url(*g, indicator_lst, obs_level, worker_char, private, key), s)
                for g in _url_groups(firm_char, private, state_lst, fips_lst)
            )
        )

    s.close()
    
    if ('06' in state_lst) and obs_level == 'msa':
        df = df.append(_LA_fetch_data(firm_char, worker_char))

    return df


def _cols_to_numeric(df, var_lst):
    df[var_lst] = df[var_lst].apply(pd.to_numeric, errors='ignore')
    return df


def _annualizer(df, annualize, covars):
    if not annualize:
        return df
    elif annualize == 'March':
        df = df.\
            assign(
                quarter=lambda x: x['time'].str[-1:],
                time=lambda x: x.apply(lambda y: int(y['time'][:4]) - 1 if y['quarter'] == '1' else int(y['time'][:4]), axis=1)
            ).\
            astype({'time': 'str'}).\
            drop('quarter', 1)
    else:
        df = df. \
            assign(
                time=lambda x: x['time'].str[:4].astype(int),
            )
    return df. \
        assign(
            row_count=lambda x: x['fips'].groupby([x[var] for var in covars], dropna=False).transform('count')
        ). \
        query('row_count == 4'). \
        drop(columns=['row_count']). \
        groupby(covars).apply(lambda x: pd.DataFrame.sum(x.set_index(covars), skipna=False)).\
        reset_index(drop=False)
    # pipe(lambda x: print(x.head())). \
        # groupby(covars).apply(lambda x: pd.DataFrame.sum(x.set_index(covars), skipna=False)).\  # this line is so we get a nan if a value is missing


def _covar_create_fips_region(df, region):
    if region == 'state':
        df['fips'] = df['state'].astype(str)
    elif region == 'county':
        df['fips'] = df['state'].astype(str) + df['county'].astype(str)
    elif region == 'msa':
        df['fips'] = df['metropolitan statistical area/micropolitan statistical area'].astype(str)
    else:
        df = df.assign(fips='00')
    return df.assign(region=lambda x: x['fips'].map(c.all_fips_to_name))


def _obs_filter_strata_totals(df, firm_char, worker_char, strata_totals):
    strata = firm_char + worker_char
    df = df.astype(dict(zip(strata, ['string'] * len(strata))))

    if not strata_totals:
        for stratum in strata:
            if stratum == 'industry':
                df.query(f'industry != "00"', inplace=True)
            elif stratum == 'agegrp':
                df.query(f'agegrp != "A00"', inplace=True)
            elif stratum == 'education':
                df.query(f'education != "E0"', inplace=True)
            elif stratum == 'race':
                df.query(f'(race != "A0") and (ethnicity != "A0")', inplace=True)
            else:
                df.query(f'{stratum} != "0"', inplace=True)
    return df


def _obs_filter_groupby_msa(df, covars, region):
    if region != 'msa':
        return df
    return df. \
        assign(
            msa_states=lambda x: x[['state', 'fips']].groupby('fips').transform(lambda y: len(y.unique().tolist())),
            time_count=lambda x: x[covars + ['msa_states']].groupby(covars).transform('count')
        ). \
        query('time_count == msa_states'). \
        drop(columns=['msa_states', 'time_count']).\
        groupby(covars).sum().\
        reset_index(drop=False)


def _state_overlap(x, state_lst0):
    if list(set(x.split(', ')[1].split('-')) & set(state_lst0)):
        return 1
    else:
        return 0


def _obs_filter_msa_state_lst(df, state_lst, state_lst0):
    if sorted(state_lst) == sorted(state_lst0):
        return df
    else:
        return df.\
            assign(_keep=lambda x: x['region'].apply(lambda y: _state_overlap(y, [c.state_fips_to_abb[fips] for fips in state_lst0]))).\
            query('_keep == 1').\
            drop('_keep', 1)


def _qwi_data_create(indicator_lst, region, state_lst, fips_list, private, annualize, firm_char, worker_char, strata_totals, key, n_threads, state_lst0=None):
    covars = ['time', 'fips', 'region', 'ownercode'] + firm_char + worker_char

    state_lst0 = state_lst
    if (len(state_lst) < 51) and (region == 'msa') and not fips_list:
        state_lst = state_msa_cross_walk(state_lst, 'metro')['fips_state'].unique().tolist()

    if region == 'us':
        df = _us_fetch_data(private, firm_char, worker_char). \
            assign(
                time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan
            ). \
            rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})
    else:
        if fips_list and region == 'msa':
            df = _county_msa_state_fetch_data(indicator_lst, region, firm_char, worker_char, private, key, n_threads, fips_lst=[tuple(row) for row in fips_state_cross_walk(fips_list, region).values])
        elif fips_list and region == 'county':
            df = _county_msa_state_fetch_data(indicator_lst, region, firm_char, worker_char, private, key, n_threads, fips_lst=list(zip([x[:2] for x in fips_list], fips_list)))
        else:
            df = _county_msa_state_fetch_data(indicator_lst, region, firm_char, worker_char, private, key, n_threads, state_lst=state_lst)

    return df. \
        pipe(_covar_create_fips_region, region).\
        pipe(_cols_to_numeric, indicator_lst). \
        pipe(_obs_filter_strata_totals, firm_char, worker_char, strata_totals). \
        pipe(_obs_filter_groupby_msa, covars, region). \
        pipe(_obs_filter_msa_state_lst, state_lst, state_lst0) \
        [covars + indicator_lst].\
        pipe(_annualizer, annualize, covars).\
        sort_values(covars).\
        reset_index(drop=True)


def qwi_estimate_shape(indicator_lst, region_lst, firm_char, worker_char, strata_totals, state_list, fips_list):
    n_columns = len(indicator_lst + firm_char + worker_char + ['time', 'fips', 'region', 'ownercode'])
    row_estimate = 0

    for region in region_lst:
        if region == 'us':
            year_regions = 28
        elif region == 'state':
            year_regions = pd.DataFrame(c.qwi_start_to_end_year()). \
                T.reset_index(). \
                rename(columns={'index':'state'}). \
                astype({'start_year':'int', 'end_year':'int'}). \
                query(f'state in {state_list}'). \
                assign(n_years=lambda x: x['end_year'] - x['start_year'] + 1) \
                ['n_years'].sum()
        elif fips_list or region in ['msa', 'county']:
            d = c.qwi_start_to_end_year()
            query = f"fips_{region} in {fips_list}" if fips_list else f"fips_state in {state_list}"
            year_regions = load_CBSA_cw(). \
                query(query) \
                [[f'fips_{region}', 'fips_state']]. \
                drop_duplicates(). \
                groupby('fips_state').count(). \
                reset_index(). \
                assign(
                    n_years=lambda x: x['fips_state']. \
                        map(lambda y: int(d[y]['end_year']) - int(d[y]['start_year']) + 1),
                    year_regions=lambda x: x['n_years']*x[f'fips_{region}']
                ) \
                ['year_regions'].sum()

        # Get n_strata_levels
        strata_levels = 1
        strata = worker_char + firm_char
        strata_to_nlevels = {
            'firmage':5, 'firmsize':5, 'industry':17, 'sex':2, 'agegrp':8, 'race':6, 'ethnicity':2, 'education':5
        }
        if strata_totals:
            strata_to_nlevels = {k:v + 1 for k,v in strata_to_nlevels.items()}

        for s in strata:
            strata_levels *= strata_to_nlevels[s]
        
        row_estimate += year_regions*strata_levels*4

    return (row_estimate, n_columns)


def qwi(indicator_lst='all', obs_level='all', state_list='all', fips_list=[], private=False, annualize='January', firm_char=[], worker_char=[], strata_totals=False, key=os.getenv("CENSUS_KEY"), n_threads=1):
    """
    Fetches nation-, state-, MSA-, or county-level Quarterly Workforce Indicators (QWI) data either from the LED
    extractor tool in the case of national data (https://ledextract.ces.census.gov/static/data.html) or from the
    Census's API in the case of state, MSA, or county (https://api.census.gov/data/timeseries/qwi/sa/examples.html).
    FTP: https://lehd.ces.census.gov/data/qwi/R2021Q1/us/

    obs_level: str
        'state': resident population of state from 1990 through 2019
        'msa': resident population of msa from 1990 through 2019
        'county': resident population of county from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019
        'all': default, returns data on all of the above observation levels

    indicator_lst: str, lst
        'all': default, will return all QWI indicaotrs;
        otherwise: return list of indicators plus 'time', 'ownercode', 'firmage', and 'fips'

        # todo: alphabetize this list
        EmpSpv: Full-Quarter Employment in the Previous Quarter: Counts
        SepBeg: Beginning-of-Quarter Separations
        EmpS: Full-Quarter Employment (Stable): Counts
        FrmJbLsS: Firm Job Loss (Stable): Counts
        HirAEndReplr: Replacement Hiring Rate
        HirAEnd: End-of-Quarter Hires
        FrmJbLs: Firm Job Loss: Counts (Job Destruction)
        EarnS: Full Quarter Employment (Stable): Average Monthly Earnings
        HirR: Hires Recalls: Counts
        FrmJbC: Firm Job Change:Net Change
        Emp: Beginning-of-Quarter Employment: Counts
        FrmJbGnS: Firm Job Gains (Stable): Counts
        HirAs: Hires All (Stable): Counts (Flows into Full-QuarterEmployment)
        SepSnx: Separations (Stable), Next Quarter: Counts (Flow out of Full-Quarter Employment)
        HirNs: Hires New (Stable): Counts (New Hires to Full-Quarter Status)
        Sep: Separations: Counts
        EarnHirAS: Hires All (Stable): Average Monthly Earnings
        Payroll: Total Quarterly Payroll: Sum
        HirA: Hires All: Counts (Accessions)
        FrmJbCS: Job Change (Stable): Net Change
        EmpTotal: Employment-Reference Quarter: Counts
        HirAEndRepl: Replacement Hires
        EarnHirNS: Hires New (Stable): Average Monthly Earnings
        TurnOvrS: Turnover (Stable)
        HirN: Hires New: Counts
        EarnBeg: End-of-Quarter Employment: Average Monthly Earnings
        EmpEnd: End-of-Quarter Employment: Counts
        SepBegR: Beginning-of-Quarter Separation Rate
        EarnSepS: Separations (Stable): Average Monthly Earnings
        HirAEndR: End-of-Quarter Hiring Rate
        SepS: Separations (Stable): Counts (Flow out of Full-Quarter Employment)
        FrmJbGn: Firm Job Gains: Counts (Job Creation)

        ***HirAEndRepl  HirAEndReplr are not available for US

    state_list: str, lst
        'all': default, includes all US states and D.C.
        otherwise: a state or list of states, identified using postal code abbreviations

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
        region_lst = [obs_level]
    elif obs_level == 'all':
        region_lst = ['us', 'state', 'county', 'msa']
    else:
        print('Invalid input to obs_level.')

    # todo: should I allow for state_list and fips_list together?
    #     I think we want some logic that makes only one of state_list and fips_list nonempty, and change the default parameter values to empty lists
    if state_list == 'all':
        state_list = [c.state_abb_to_fips[s] for s in c.states]
    else:
        state_list = [c.state_abb_to_fips[s] for s in state_list]

    if indicator_lst == 'all':
        indicator_lst = c.qwi_outcomes
    elif type(indicator_lst) == str:
        indicator_lst = [indicator_lst]

    # todo: keep this?
    # if annualize and any(x in c.qwi_averaged_outcomes for x in indicator_lst):
    #     raise Exception(f'{indicator_lst} is not compatible with annualize==True')


    firm_char = [firm_char] if type(firm_char) == str else firm_char
    private = True if any(x in ['firmage', 'firmsize'] for x in firm_char) else private
    if obs_level in ['us', 'all'] and private == False:
        private = True
        print("Warning: US-level data is only available when private=True. Variable 'private' has been set to True.")

    worker_char = [worker_char] if type(worker_char) == str else worker_char

    if set(worker_char) not in [
        {'sex', 'agegrp'}, {'sex', 'education'}, {'education'}, {'ethnicity', 'race'}, 
        {'sex'}, {'agegrp'}, {'race'}, {'ethnicity'}, set()
    ]:
        raise Exception('Invalid input to worker_char. See function documentation for valid groups.')

    if 'firmage' in firm_char and 'firmsize' in firm_char:
        raise Exception('Invalid input to firm_char. Can only specify one of firmage or firmsize.')

    strata_totals = False if not (firm_char or worker_char) else strata_totals

    if fips_list and any([region not in ['msa', 'county'] for region in region_lst]):
        raise Exception('If fips_list is provided, region must be either msa or county.')

    estimated_shape = qwi_estimate_shape(indicator_lst, region_lst, firm_char, worker_char, strata_totals, state_list, fips_list)
    if estimated_shape[0] * estimated_shape[1] > 100000000:
        print(f'Warning: You are attempting to fetch a dataframe of estimated shape {estimated_shape}. You may experience memory errors.')

    return pd.concat(
            [
                _qwi_data_create(indicator_lst, region, state_list, fips_list, private, annualize, firm_char, worker_char, strata_totals, key, n_threads)
                for region in region_lst
            ],
            axis=0
        )

