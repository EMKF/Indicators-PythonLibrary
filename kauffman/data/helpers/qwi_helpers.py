import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import time
import requests
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


def _region_year_lst(obs_level, start_year, end_year):
    years = list(range(max(start_year, 2000), min(end_year, 2019) + 1))
    if obs_level in ['state', 'county']:
        return list(product(c.state_abb_fips_dic.values(), years))
    if obs_level == 'msa':
        return list(product([(k, state) for k, states_lst in c.msa_fips_state_fips_dic.items() for state in states_lst], years))
        # return list(product(c.msa_fips_state_fips_dic.items(), years))


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
    r = requests.get(url)
    try:
        df = pd.DataFrame(r.json()).pipe(_build_df_header)
        print('Success')
        # return pd.DataFrame(r.json()).pipe(lambda x: x.rename(columns=dict(zip(x.columns, x.iloc[0]))))[1:]  # essentially the same as above; the rename function does not, apparently, give access to df
    except:
        print('Fail\n')
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
            for syq in _region_year_lst(obs_level, start_year, end_year)  #[-40:]
        ]
    )


def _us_fetch_data_all(private, by_age, strat):
    print('\tFiring up selenium extractor...')
    pause1 = 1
    pause2 = 3

    chrome_options = Options()
    chrome_options.add_argument('--headless')

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get('https://ledextract.ces.census.gov/static/data.html')

    # Geography
    print('\tGeography tab...')
    time.sleep(pause1)
    driver.find_element_by_id('continue_with_selection_label').click()

    # Firm Characteristics
    print('\tFirm Characteristics tab...')
    if private:
        driver.find_element_by_id('dijit_form_RadioButton_4').click()
    if by_age:
        for box in range(0, 6):
            driver.find_element_by_id('dijit_form_CheckBox_{}'.format(box)).click()
            time.sleep(pause1)
        time.sleep(pause1)
    if 'industry' in strat:
        elems = driver.find_elements_by_xpath("//a[@href]")[13]
        driver.execute_script("arguments[0].click();", elems)
    driver.find_element_by_id('continue_to_worker_char').click()

    # Worker Characteristics
    print('\tWorker Characteristics tab...')
    if 'sex' in strat:
        # driver.find_element_by_id('dijit_form_CheckBox_12').click()
        driver.find_element_by_id('dijit_form_CheckBox_13').click()
        driver.find_element_by_id('dijit_form_CheckBox_14').click()
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
    time.sleep(pause2)
    driver.find_element_by_id('submit_request').click()
    try:
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element_by_link_text('CSV').get_attribute('href')
        return pd.read_csv(href)


def _msa_year_filter(df):
    return df. \
        assign(
            msa_states=lambda x: x[['state', 'fips']].groupby('fips').transform(lambda y: len(y.unique().tolist())),
            time_count=lambda x: x[['fips', 'time', 'firmage', 'msa_states']].groupby(['fips', 'time', 'firmage']).transform('count')
        ). \
        query('time_count == msa_states'). \
        drop(columns=['msa_states', 'time_count']).\
        reset_index(drop=True)


def _msa_combiner(df):
    return df.groupby(['fips', 'firmage', 'time']).sum().reset_index(drop=False)


def _annualizer(df, annualize, strat):
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
                time=lambda x: x['time'].str[:4],
            )

    groupby_vars = ['fips', 'time', 'firmage'] if not strat else ['fips', 'time', 'firmage'] + strat
    return df. \
        assign(
            row_count=lambda x: x['fips'].groupby([x[var] for var in groupby_vars]).transform('count')
        ). \
        query('row_count == 4'). \
        drop(columns=['row_count']). \
        groupby(groupby_vars).apply(lambda x: pd.DataFrame.sum(x.set_index(groupby_vars), skipna=False)).\
        reset_index(drop=False)


def _qwi_data_create(indicator_lst, region, start_year, end_year, private, by_age, annualize, strata):
    if region == 'state':
        df = _county_msa_state_fetch_data_all(region, start_year, end_year). \
            astype({'state': 'str'}). \
            rename(columns={'state': 'fips'})
    elif region == 'county':
        df = _county_msa_state_fetch_data_all(region, start_year, end_year). \
            assign(fips=lambda x: x['state'].astype(str) + x['county'].astype(str)). \
            drop(['state', 'county'], 1)
    elif region == 'msa':
        df = _county_msa_state_fetch_data_all(region, start_year, end_year). \
            rename(columns={'metropolitan statistical area/micropolitan statistical area': 'fips'}). \
            pipe(_msa_year_filter). \
            drop('state', 1)
    else:
        ui_covars = [
            'Emp', 'EmpEnd', 'EmpS', 'EmpSpv', 'EmpTotal', 'HirA', 'HirN', 'HirR', 'Sep', 'HirAEnd', 'HirAEndR',
            'SepBeg', 'SepBegR', 'HirAs', 'HirNs', 'SepS', 'SepSnx', 'TurnOvrS', 'FrmJbGn', 'FrmJbLs', 'FrmJbC',
            'FrmJbGnS', 'FrmJbLsS', 'FrmJbCS', 'EarnS', 'EarnBeg', 'EarnHirAS', 'EarnHirNS', 'EarnSepS', 'Payroll',
            'time', 'ownercode', 'firmage', 'fips'
        ]
        df = _us_fetch_data_all(private, by_age, strat=strata). \
            assign(
            time=lambda x: x['year'].astype(str) + '-Q' + x['quarter'].astype(str),
            fips='00'
        ). \
            rename(columns={'geography': 'region', 'HirAS': 'HirAs', 'HirNS': 'HirNs'}) \
            [ui_covars if not strata else ui_covars + strata]
    #             query('{start_year}<=year<={end_year}'.format(start_year=max(start_year, 2000), end_year=min(end_year, 2019))).\  # todo: why did I do this here? Oh, the other region queries have dates built in

    # todo: look for a better way to do this list garbage.
    covars = ['time', 'firmage', 'fips'] if not strata else ['time', 'firmage', 'fips'] + strata
    indicator_lst = [col for col in df.columns.tolist() if
                     col not in covars + ['ownercode']] if not indicator_lst else indicator_lst
    return df. \
        reset_index(drop=True) \
        [indicator_lst + covars]. \
        astype(dict(zip(indicator_lst, ['float'] * len(indicator_lst)))). \
        pipe(_msa_combiner if region == 'msa' else lambda x: x). \
        pipe(_annualizer, annualize, strata)
    # pipe(_annualizer if annualize else lambda x: x)

# todo: print statements etc.