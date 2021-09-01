import os
import sys
import time
import requests
import numpy as np
import pandas as pd
from itertools import product
from kauffman import constants as c
from ..tools._etl import state_msa_cross_walk
from webdriver_manager.chrome import ChromeDriverManager

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


def _state_year_lst(state_lst):
    out_lst = []
    df = c.qwi_start_to_end_year()
    for state in state_lst:
        start_year = int(df[state]['start_year'])
        end_year = int(df[state]['end_year'])
        out_lst += list(product([state], range(start_year, end_year + 1)))
    return out_lst


def _build_strata_url(strata):
    url_section = ''
    
    if 'firmage' in strata:
        for f in range(0, 6):
            url_section = url_section + f'&firmage={f}'
    if 'firmsize' in strata:
        for f in range(0, 6):
            url_section = url_section + f'&firmsize={f}'
    if 'industry' in strata:
        for i in ['00', 11, 21, 22, 23, 42, 51, 52, 53, 54, 55, 56, 61, 62, 71, 72, 81, 92]:
            url_section = url_section + f'&industry={i}'
    if 'sex' in strata:
        url_section = url_section + '&sex=0&sex=1&sex=2'
    if 'agegrp' in strata:
        for age in range(0, 9):
            url_section = url_section + f'&agegrp=A0{age}'

    return url_section


def _build_url(fips, year, region, bds_key, firm_strat):
    base_url = 'https://api.census.gov/data/timeseries/qwi/sa?'
    var_lst = ','.join(c.qwi_outcomes)
    strata_section = _build_strata_url(firm_strat)

    if region == 'msa':
        for_region = f'for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&in=state:{fips}'
    elif region == 'county':
        for_region = f'for=county:*&in=state:{fips}'
    else:
        for_region = f'for=state:{fips}'
    return '{0}get={1}&{2}&time={3}&ownercode=A05{4}&key={5}'. \
        format(base_url, var_lst, for_region, year, strata_section, bds_key)


def _build_df_header(df):
    df.columns = df.iloc[0]
    return df[1:]


def _fetch_from_url(url):
    r = requests.get(url)
    try:
        df = pd.DataFrame(r.json()).pipe(_build_df_header)
    except:
        print('Fail', r, url)
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
        driver.find_elements_by_link_text('California')[0].click()
        time.sleep(pause2)
        driver.find_element_by_xpath('//*[@title="Click to select Micro/Metropolitan Areas"]').click()
        time.sleep(pause2)
        driver.find_element_by_xpath('//*[@title="Click to select 06 California"]').click()
        driver.find_element_by_xpath('//*[@title="Click to select 0631080 Los Angeles-Long Beach-Anaheim, CA"]').click()
    time.sleep(pause1)
    driver.find_element_by_id('continue_with_selection_label').click()

    # Firm Characteristics
    if private:
        driver.find_element_by_id('dijit_form_RadioButton_4').click()

    if 'firmage' in firm_char:
        for box in range(1, 6):
            driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()

    if 'firmsize' in firm_char:
        for box in range(7, 12):
            driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()

    if 'industry' in firm_char:
        elems = driver.find_elements_by_xpath("//a[@href]")[12]
        driver.execute_script("arguments[0].click();", elems)
    driver.find_element_by_id('continue_to_worker_char').click()

    # Worker Characteristics
    if 'sex' in worker_char:
        driver.find_element_by_id('dijit_form_CheckBox_13').click()
        driver.find_element_by_id('dijit_form_CheckBox_14').click()
    if 'agegrp' in worker_char:
        for box in range(1, 9):
            driver.find_element_by_xpath(f'//input[@value="A0{box}"]').click()
    if 'education' in worker_char:
        driver.find_element_by_id('worker_char_switch').click()
        driver.find_element_by_id('dijit_MenuItem_2').click()
        for box in range(1, 6):
            driver.find_element_by_xpath(f'//input[@value="E{box}"]').click()
    if 'race' in worker_char:
        driver.find_element_by_id('worker_char_switch').click()
        driver.find_element_by_id('dijit_MenuItem_3').click()
        for box in range(1, 8):
            for element in driver.find_elements_by_xpath(f'//input[@value="A{box}"]'):
                element.click()
    driver.find_element_by_id('continue_to_indicators').click()

    # Indicators
    for _ in range(0, 3):
        driver.find_element_by_class_name('ClosedGroup').click()
        time.sleep(pause2)
    for box in range(19, 50):
        driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()
        # time.sleep(pause1)
    driver.find_element_by_id('continue_to_quarters').click()

    # Quarters
    for quarter in range(1, 5):
        driver.find_element_by_xpath('//*[@title="Check All Q{}"]'.format(quarter)).click()
    driver.find_element_by_id('continue_to_export').click()

    # Summary and Export
    time.sleep(pause2)
    driver.find_element_by_id('submit_request').click()

    try:
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element_by_link_text('CSV').get_attribute('href')
        return pd.read_csv(href)


def _us_fetch_data(private, firm_char, worker_char):
    return _qwi_ui_fetch_data(private, firm_char, worker_char)


def _LA_fetch_data(strata):
    df = _qwi_ui_fetch_data(True, strata, region='LA'). \
            assign(
                time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan,
                state='06'
            ). \
            rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})
    df['metropolitan statistical area/micropolitan statistical area'] = '31080'
    return df


def _county_msa_state_fetch_data(obs_level, state_lst, strata):
    df = pd.concat(
        [
            _fetch_from_url(_build_url(syq[0], syq[1], obs_level, os.getenv('BDS_KEY'), strata))
            for syq in _state_year_lst(state_lst)
        ]
    )

    if ('06' in state_lst) and obs_level == 'msa':
        df = df.append(_LA_fetch_data(strata))

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


def _qwi_data_create(indicator_lst, region, state_lst, private, annualize, firm_char, worker_char, strata_totals):
    covars = ['time', 'fips', 'region', 'ownercode'] + firm_char + worker_char

    if region == 'us':
        df = _us_fetch_data(private, firm_char, worker_char). \
            assign(
                time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan
            ). \
            rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})
    else:
        df = _county_msa_state_fetch_data(region, state_lst, firm_char, worker_char)

    return df. \
        pipe(_covar_create_fips_region, region).\
        pipe(_cols_to_numeric, indicator_lst). \
        pipe(_obs_filter_strata_totals, firm_char, worker_char, strata_totals). \
        pipe(_obs_filter_groupby_msa, covars, region) \
        [covars + indicator_lst].\
        pipe(_annualizer, annualize, covars).\
        sort_values(covars).\
        reset_index(drop=True)


def qwi(indicator_lst='all', obs_level='all', state_list='all', private=False, annualize='January', firm_char=[], worker_char=[], strata_totals=False):
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

        # 'sex': stratify by worker sex
        'education': stratify by worker education
    # todo: need to make it so these can be crossed

        'race': worker race
        'ethnicity': workr ethnicity
    """

    if obs_level in ['us', 'state', 'county', 'msa']:
        region_lst = [obs_level]
    elif obs_level == 'all':
        region_lst = ['us', 'state', 'county', 'msa']
    else:
        print('Invalid input to obs_level.')

    # todo: is this done correctly?
    if state_list == 'all':
        state_list = [c.state_abb_to_fips[s] for s in c.states]
    elif isinstance(state_list, list) and obs_level == 'msa':
        state_list = state_msa_cross_walk(state_list, 'metro')['fips_state'].unique().tolist()
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

    worker_char = [worker_char] if type(worker_char) == str else worker_char

    strata_totals = False if not (firm_char or worker_char) else strata_totals

    return pd.concat(
            [
                _qwi_data_create(indicator_lst, region, state_list, private, annualize, firm_char, worker_char, strata_totals)
                for region in region_lst
            ],
            axis=0
        )


if __name__ == '__main__':
    # indicator_lst, region, state_lst, private, annualize, firm_char, worker_char, strata_totals
    df = _qwi_data_create(c.qwi_outcomes, 'us', [], False, False, [], ['edgrp'], False)
    # df = _qwi_data_create(c.qwi_outcomes, 'msa', ['06'], True, False, ['firmsize', 'industry'], False)
    print(df.head(100))
    print(df.tail(100))
    # _county_msa_state_fetch_data('msa', ['06'], strata=['firmsize', 'industry'])
    # _LA_fetch_data(['firmsize', 'industry'])
