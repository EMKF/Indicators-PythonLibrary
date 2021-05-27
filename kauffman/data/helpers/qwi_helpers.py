"""
https://lehd.ces.census.gov/applications/help/led_extraction_tool.html#!qwi
"""

# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context  # what is this? I forgot

import os
import sys
import time
import requests
import numpy as np
import pandas as pd
from itertools import product
from kauffman import constants as c
from webdriver_manager.chrome import ChromeDriverManager

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _state_year_lst(state_lst):
    out_lst = []
    for state in state_lst:
        start_year = int(c.qwi_start_end_year_dic[state]['start_year'])
        end_year = int(c.qwi_start_end_year_dic[state]['end_year'])
        out_lst += list(product([state], range(start_year, end_year + 1)))
    return out_lst


def _build_strata_url(strata):
    url_section = ''
    
    if 'firmage' in strata:
        for f in range(0,6):
            url_section = url_section + f'&firmage={f}'
    if 'firmsize' in strata:
        for f in range(0,6):
            url_section = url_section + f'&firmsize={f}'
    if 'sex' in strata:
        url_section = url_section + '&sex=0&sex=1&sex=2'
    if 'industry' in strata:
        for i in [11, 21, 22, 23, 42, 51, 52, 53, 54, 55, 56, 61, 62, 71, 72, 81, 92]:
            url_section = url_section + f'&industry={i}'

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


def _county_msa_state_fetch_data(obs_level, state_lst, strata):
    return pd.concat(
        [
            _fetch_from_url(
                _build_url(syq[0], syq[1], obs_level, os.getenv('BDS_KEY'), strata),
            )
            for syq in _state_year_lst(state_lst)[:5]
        ]
    )


def _us_fetch_data(private, strata):
    pause1 = 1
    pause2 = 3

    chrome_options = Options()
    chrome_options.add_argument('--headless')

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get('https://ledextract.ces.census.gov/static/data.html')

    # Geography
    time.sleep(pause1)
    driver.find_element_by_id('continue_with_selection_label').click()

    # Firm Characteristics
    if private:
        driver.find_element_by_id('dijit_form_RadioButton_4').click()

    # todo: fix this
    # if any(x in ['firmage', 'firmsize'] for x in strata):
    if 'firmage' in strata:
        for box in range(0, 6):
            driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()

    if 'industry' in strata:
        elems = driver.find_elements_by_xpath("//a[@href]")[12]
        driver.execute_script("arguments[0].click();", elems)
    driver.find_element_by_id('continue_to_worker_char').click()

    # Worker Characteristics
    if 'sex' in strata:
        # driver.find_element_by_id('dijit_form_CheckBox_12').click()
        driver.find_element_by_id('dijit_form_CheckBox_13').click()
        driver.find_element_by_id('dijit_form_CheckBox_14').click()
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


def _cols_to_numeric(df, indicator_lst):
    df[indicator_lst] = df[indicator_lst].apply(pd.to_numeric, errors='ignore')
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
            row_count=lambda x: x['fips'].groupby([x[var] for var in covars]).transform('count')
        ). \
        query('row_count == 4'). \
        drop(columns=['row_count']). \
        groupby(covars).apply(lambda x: pd.DataFrame.sum(x.set_index(covars), skipna=False)).\
        reset_index(drop=False)
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
    return df.assign(region=lambda x: x['fips'].map(c.all_fips_name_dic))


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


def _qwi_data_create(indicator_lst, region, state_lst, private, annualize, strata):
    covars = ['time', 'fips', 'region', 'ownercode'] + strata

    if region != 'us':
        df = _county_msa_state_fetch_data(region, state_lst, strata)
    else:  # region == 'us'
        df = _us_fetch_data(private, strata). \
            assign(
                time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
                HirAEndRepl=np.nan,
                HirAEndReplr=np.nan
            ). \
            rename(columns={'HirAS': 'HirAs', 'HirNS': 'HirNs'})

    return df. \
        pipe(_covar_create_fips_region, region). \
        pipe(_cols_to_numeric, indicator_lst). \
        pipe(_obs_filter_groupby_msa, covars, region) \
        [covars + indicator_lst].\
        pipe(_annualizer, annualize, covars).\
        sort_values(covars).\
        reset_index(drop=True)
