import os
import sys
import time
import joblib
import requests
import pandas as pd
from itertools import product
from kauffman_data import constants as c
import kauffman_data.cross_walk as cw

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


def _region_year_lst(obs_level, start_year, end_year):
    years = list(range(max(start_year, 2000), min(end_year, 2019) + 1))
    if obs_level in ['state', 'county']:
        return list(product(c.state_abb_fips_dic.values(), years))
    if obs_level == 'msa':
        return list(product(c.msa_fips_state_fips_dic.items(), years))


def _build_url(fips, year, region, bds_key, firm_strat='firmage'):
    if region == 'msa':
        # for_region = 'for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&in=state:{0}'.format(state)
        for_region = 'for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:{msa_fips}&in=state:{state_fips}'.format(msa_fips=fips[0], state_fips=fips[1])
    elif region == 'county':
        for_region = 'for=county:*&in=state:{0}'.format(fips)
    else:
        for_region = 'for=state:{0}'.format(fips)

    return 'https://api.census.gov/data/timeseries/qwi/sa?get=Emp,EmpEnd,EmpS,EmpTotal,EmpSpv,HirA,HirN,HirR,Sep,HirAEnd,SepBeg,HirAEndRepl,HirAEndR,SepBegR,HirAEndReplr,HirAs,HirNs,SepS,SepSnx,TurnOvrS,FrmJbGn,FrmJbLs,FrmJbC,FrmJbGnS,FrmJbLsS,FrmJbCS,EarnS,EarnBeg,EarnHirAS,EarnHirNS,EarnSepS,Payroll&{0}&time={1}&ownercode=A05&{2}=1&{2}=2&{2}=3&{2}=4&{2}=5&key={3}'.format(for_region, year, firm_strat, bds_key)


def _build_df_header(df):
    df.columns = df.iloc[0]
    return df[1:]


def _fetch_from_url(url, syq):
    # print('\t{}'.format(syq), end='...')
    r = requests.get(url)
    try:
        df = pd.DataFrame(r.json()).pipe(_build_df_header)
        # print('Success')
        # return pd.DataFrame(r.json()).pipe(lambda x: x.rename(columns=dict(zip(x.columns, x.iloc[0]))))[1:]  # essentially the same as above; the rename function does not, apparently, give access to df
    except:
        # print('Fail\n')
        df = pd.DataFrame()
    return df


def _county_msa_state_fetch_data_all(obs_level, start_year, end_year):
    print('\tQuerying the Census QWI API...')
    return pd.concat(
        [
            _fetch_from_url(
                _build_url(syq[0], syq[1], obs_level, os.getenv('BDS_KEY')),
                syq
            )
            for syq in _region_year_lst(obs_level, start_year, end_year)  #[:5]
        ]
    )


def _us_fetch_data_all():
    print('\tFiring up selenium extractor...')
    pause1 = 1
    pause2 = 3

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(
        executable_path=c.filenamer('chromedriver'), options=chrome_options
    )
    driver.get('https://ledextract.ces.census.gov/static/data.html')

    # Geography
    print('\tGeography tab...')
    time.sleep(pause1)
    driver.find_element_by_id('continue_with_selection_label').click()

    # Firm Characteristics
    print('\tFirm Characteristics tab...')
    driver.find_element_by_id('dijit_form_RadioButton_4').click()
    for box in range(0, 6):
        driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()
        time.sleep(pause1)
    time.sleep(pause1)
    driver.find_element_by_id('continue_to_worker_char').click()

    # Worker Characteristics
    print('\tWorker Characteristics tab...')
    driver.find_element_by_id('continue_to_indicators').click()

    # Indicators
    print('\tIndicators tab...')
    for _ in range(0, 3):
        driver.find_element_by_class_name('ClosedGroup').click()
        time.sleep(pause2)
    for box in range(19, 50):
        driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()
        time.sleep(pause1)
    driver.find_element_by_id('continue_to_quarters').click()

    # Quarters
    print('\tQuarters tab...')
    for quarter in range(1, 5):
        driver.find_element_by_xpath('//*[@title="Check All Q{}"]'.format(quarter)).click()
        time.sleep(pause1)
    driver.find_element_by_id('continue_to_export').click()

    # Summary and Export
    print('\tExport tab...')
    time.sleep(pause2)
    driver.find_element_by_id('submit_request').click()
    try:
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element_by_link_text('CSV').get_attribute('href')
        return pd.read_csv(href)


def get_data(obs_level, indicator_lst=None, start_year=2000, end_year=2019, annualize=False):
    """
    Fetches nation-, state-, MSA-, or county-level Quarterly Workforce Indicators (QWI) data either from the LED
    extractor tool in the case of national data (https://ledextract.ces.census.gov/static/data.html) or from the
    Census's API in the case of state, MSA, or county (https://api.census.gov/data/timeseries/qwi/sa/examples.html).

    obs_level:
        'state': resident population of state from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019

    indicator_lst: str or lst
        'All': default, will return all QWI indicaotrs;
        otherwise: return list of indicators plus 'time', 'ownercode', 'firmage', and 'fips'

    start_year: earliest start year is 2000

    end_year: latest end year is 2019
    """
    print('Extracting PEP data for {}...'.format(obs_level))

    if obs_level == 'state':
        df = _county_msa_state_fetch_data_all(obs_level, start_year, end_year).\
            astype({'state': 'str'}).\
            rename(columns={'state': 'fips'})
    elif obs_level == 'county':
        df = _county_msa_state_fetch_data_all(obs_level, start_year, end_year).\
            assign(fips=lambda x: x['state'].astype(str) + x['county'].astype(str)).\
            drop(['state', 'county'], 1)
    elif obs_level == 'msa':
        df = _county_msa_state_fetch_data_all(obs_level, start_year, end_year).\
            rename(columns={'metropolitan statistical area/micropolitan statistical area': 'fips'}).\
            drop('state', 1)
    else:
        df = _us_fetch_data_all().\
            query('{start_year}<=year<={end_year}'.format(start_year=max(start_year, 2000), end_year=min(end_year, 2019))).\
            assign(
                time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
                fips='00'
            ).\
            rename(columns={'geography': 'region', 'HirAS': 'HirAs', 'HirNS': 'HirNs'}) \
            [['Emp', 'EmpEnd', 'EmpS', 'EmpSpv', 'EmpTotal', 'HirA', 'HirN',
              'HirR', 'Sep', 'HirAEnd', 'HirAEndR', 'SepBeg', 'SepBegR', 'HirAs', 'HirNs', 'SepS', 'SepSnx',
              'TurnOvrS', 'FrmJbGn', 'FrmJbLs', 'FrmJbC', 'FrmJbGnS', 'FrmJbLsS', 'FrmJbCS', 'EarnS', 'EarnBeg',
              'EarnHirAS', 'EarnHirNS', 'EarnSepS', 'Payroll', 'time', 'ownercode', 'firmage', 'fips']]

    covars = ['time', 'firmage', 'fips']
    indicator_lst = [col for col in df.columns.tolist() if col not in covars + ['ownercode']] if not indicator_lst else indicator_lst
    return df.\
        reset_index(drop=True) \
        [indicator_lst + covars].\
        astype(dict(zip(indicator_lst, ['float'] * len(indicator_lst)))).\
        pipe(_annualizer if annualize else lambda x: x)


def _annualizer(df):
    return df. \
        assign(
            time=lambda x: x['time'].str[:4],
            row_count=lambda x: x['fips'].groupby([x['fips'], x['time'], x['firmage']]).transform('count')
        ). \
        query('row_count == 4'). \
        drop(columns=['row_count']).\
        groupby(['fips', 'firmage', 'time']).sum().\
        reset_index(drop=False)


if __name__ == '__main__':
    # get_data('county', 2014, 2016)  #.to_csv('/Users/thowe/Downloads/pep_county.csv', index=False)
    # get_data('msa', 2015, 2016)  #.to_csv('/Users/thowe/Downloads/pep_msa.csv', index=False)
    # get_data('us', 2015, 2016)  #.to_csv('/Users/thowe/Downloads/pep_us.csv', index=False)
    # df = get_data('state', indicator_lst=['Emp', 'EmpS'], start_year=2014, end_year=2020, annualize=True)  #.pipe(data_transformer)

    df = get_data('msa', indicator_lst=['Emp', 'EmpS'], start_year=2014, end_year=2015, annualize=True)  #.pipe(data_transformer)
    print(df)

# todo: plot the MSA data
    # 31080 is LA

# todo: specify public or private
# todo: does annualize work for all indicators? Some might not because of how they are defined.
