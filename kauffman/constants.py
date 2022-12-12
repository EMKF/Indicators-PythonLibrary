import pandas as pd
import geonamescache
from itertools import product
import os

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY"
]

STATE_ABB_TO_FIPS = {
    'WA': '53', 'DE': '10', 'DC': '11', 'WI': '55', 'WV': '54', 'HI': '15',
    'FL': '12', 'WY': '56', 'PR': '72', 'NJ': '34', 'NM': '35', 'TX': '48',
    'LA': '22', 'NC': '37', 'ND': '38', 'NE': '31', 'TN': '47', 'NY': '36',
    'PA': '42', 'AK': '02', 'NV': '32', 'NH': '33', 'VA': '51', 'CO': '08',
    'CA': '06', 'AL': '01', 'AR': '05', 'VT': '50', 'IL': '17', 'GA': '13',
    'IN': '18', 'IA': '19', 'MA': '25', 'AZ': '04', 'ID': '16', 'CT': '09',
    'ME': '23', 'MD': '24', 'OK': '40', 'OH': '39', 'UT': '49', 'MO': '29',
    'MN': '27', 'MI': '26', 'RI': '44', 'KS': '20', 'MT': '30', 'MS': '28',
    'SC': '45', 'KY': '21', 'OR': '41', 'SD': '46', 'US': '00'
}

STATE_FIPS_TO_ABB = {v: k for k, v in STATE_ABB_TO_FIPS.items()}

STATE_NAME_TO_ABB = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Palau': 'PW',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
    'United States': 'US'
}

STATE_ABB_TO_NAME = dict(map(reversed, STATE_NAME_TO_ABB.items()))

def MSA_FIPS_TO_NAME():
    return pd.read_excel(
        'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
        header=2,
        skipfooter=4,
        usecols=[0, 3],
    ) \
        .assign(fips=lambda x: x['CBSA Code'].astype(str)) \
        .drop('CBSA Code', 1) \
        .drop_duplicates('fips') \
        .set_index(['fips']) \
        .to_dict()['CBSA Title']


ALL_FIPS_TO_NAME = {
    **{
        '01001': 'Autauga County',
        '02063': 'Chugach Census Area',
        '02066': 'Copper River Census Area',
        '02158': 'Kusilvak Census Area',
        '46102': 'Oglala Lakota County'
    },
    **{
        dict['fips']: dict['name'] 
        for dict in geonamescache.GeonamesCache().get_us_counties()
    },
    **MSA_FIPS_TO_NAME(),
    **{
        k: STATE_ABB_TO_NAME[v]
        for k, v in STATE_FIPS_TO_ABB.items() if v != 'PR'
    }
}
ALL_NAME_TO_FIPS = dict(map(reversed, ALL_FIPS_TO_NAME.items()))

def QWI_START_TO_END_YEAR():
    return pd.read_html('https://ledextract.ces.census.gov/loading_status.html')[0] \
        [['State', 'Start Quarter', 'End Quarter']] \
        .assign(
            start_year=lambda x: x['Start Quarter'].str.split().str[0],
            end_year=lambda x: x['End Quarter'].str.split().str[0],
            fips=lambda x: x['State'].map(STATE_ABB_TO_FIPS),
        ) \
        .astype({'start_year':'int', 'end_year':'int'}) \
        .set_index('fips') \
        [['start_year', 'end_year']] \
        .to_dict('index')


def fetch_msa_to_state_dic():
    df = pd \
        .read_excel(
            'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
            skiprows=2, skipfooter=4,
            dtype = {'CBSA Code':'str', 'FIPS State Code':'str'}
        ) \
        .rename(columns={"CBSA Code":"fips", "FIPS State Code":"state_fips"}) \
        .drop_duplicates(['fips', 'state_fips']) \
        [['fips', 'state_fips']]

    df2 = df.pivot(values='state_fips', columns='fips')
    names = df2.columns
    values = [list(df2[col].dropna().values) for col in df2]
    return dict(zip(names, values))

MSA_TO_STATE_FIPS = fetch_msa_to_state_dic()

STATE_TO_MSA_FIPS = {}
for k, v in MSA_TO_STATE_FIPS.items():
    for x in v:
        STATE_TO_MSA_FIPS.setdefault(x,[]).append(k)


AGE_CODE_TO_LABEL = {
    0: 'Less than one year old',
    1: '1 to 4 years',
    2: '5 to 9 years',
    3: '10 years or older',
    4: 'All'
}

BED_SIZE_CODE_TO_LABEL = {
    0: '1 to 4 employees',
    1: '5 to 9 employees',
    2: '10 to 19 employees',
    3: '20 to 49 employees',
    4: '50 to 99 employees',
    5: '100 to 499 employees',
    6: '500 or more employees',
    7: 'All'
}
BED_AGE_SIZE_LIST = list(product(AGE_CODE_TO_LABEL.keys(), BED_SIZE_CODE_TO_LABEL.keys()))

BED_TABLE1BF_COLS = [
    'time', 'firms', 'establishments', 'net_change', 'total_job_gains',
    'expanding_job_gains', 'opening_job_gains', 'total_job_losses',
    'contracting_job_losses', 'closing_job_losses'
]

BED_TABLE_FIRM_SIZE_COLS = [
    'time', 'quarter', 'net_change', 'total_job_gains',
    'expanding_firms_job_gains', 'opening_firms_job_gains', 'total_job_losses',
    'contracting_firms_job_losses', 'closing_firms_job_losses'
]

BED_SIZE_CODE_TO_LABEL2 = {
    1: '1 to 4 employees',
    2: '5 to 9 employees',
    3: '10 to 19 employees',
    4: '20 to 49 employees',
    5: '50 to 99 employees',
    6: '100 to 249 employees',
    7: '250 to 499 employees',
    8: '500 to 999 employees',
    9: '1000 or more employees'
}

MONTH_TO_QUARTER = {
    'March': 1,
    'June': 2,
    'September': 3,
    'December': 4
}


API_CELL_LIMIT = 400000
API_MSA_STRING = 'metropolitan statistical area/micropolitan statistical area'

QWI_WORKER_CROSSES = [
    {'sex', 'agegrp'}, {'sex', 'education'}, {'education'}, 
    {'ethnicity', 'race'}, {'sex'}, {'agegrp'}, {'race'}, {'ethnicity'}, set()
]

QWI_STRATA_TO_NLEVELS = {
    'firmage':6, 'firmsize':6, 'industry':20, 'sex':3, 'agegrp':9,
    'race':8, 'ethnicity':3, 'education':6
}

QWI_GEO_TO_MAX_CARDINALITY = {
    'county': 254,
    'msa': 30,
    'state': 1
}

QWI_STRATA_TO_LEVELS = {
    'firmage': [x for x in range(0,6)],
    'firmsize': [x for x in range(0,6)],
    'industry': [
        '00', '11', '21', '22', '23', '31-33', '42', '44-45', '51', '52', '53',
        '54', '55', '56', '61', '62', '71', '72', '81', '92'
    ],
    'education': [f'E{i}' for i in range(0,6)],
    'sex': [x for x in range(0,3)],
    'agegrp':[f'A0{i}' for i in range(0,9)],
    'race':[f'A{i}' for i in range(0,8) if i != 6],
    'ethnicity':[f'A{i}' for i in range(0,3)]
}

QWI_OUTCOMES = [
    'EarnBeg', 'EarnHirAS', 'EarnHirNS', 'EarnS', 'EarnSepS', 'Emp', 'EmpEnd',
    'EmpS', 'EmpSpv', 'EmpTotal', 'FrmJbC', 'FrmJbCS', 'FrmJbGn', 'FrmJbGnS',
    'FrmJbLs', 'FrmJbLsS', 'HirA', 'HirAEnd', 'HirAEndR', 'HirAEndRepl',
    'HirAEndReplr', 'HirAs', 'HirN', 'HirNs', 'HirR', 'Payroll', 'Sep',
    'SepBeg', 'SepBegR', 'SepS', 'SepSnx', 'TurnOvrS'
]

QWI_MISSING_COUNTIES = {
    '02': ['063', '066', '158'], 
    '46': ['102']
}

QWI_MISSING_MSAS = {
    '01': ['10760', '12120', '21640', '22840', '27530'],
    '13': ['21640'],
    '04': ['39150'],
    '05': ['26260'],
    '17': ['36837'],
    '18': ['14160', '42500'],
    '19': ['16140', '37800'],
    '20': ['49060'],
    '21': ['16420'],
    '22': ['27660', '33380'],
    '27': ['21860', '24330'],
    '28': ['48500'],
    '33': ['30100'],
    '35': ['40760'],
    '36': ['39100'],
    '39': ['19430'],
    '42': ['41260'],
    '45': ['46420'],
    '47': ['15140'],
    '48': ['14300', '24180', '37770', '40530'],
    '50': ['30100'],
    '54': ['34350']
}


ACS_CODE_TO_VAR = {
    'B24081_001E': 'total',
    'B24081_002E': 'private',
    'B24081_003E': 'private_employee',
    'B24081_004E': 'private_self_employed',
    'B24081_005E': 'non_profit',
    'B24081_006E': 'local_government',
    'B24081_007E': 'state_government',
    'B24081_008E': 'federal_government',
    'B24081_009E': 'self_employed_not_inc',
    'B24092_001E': 'total_m',
    'B24092_002E': 'private_m',
    'B24092_003E': 'private_employee_m',
    'B24092_004E': 'private_self_employed_m',
    'B24092_005E': 'non_profit_m',
    'B24092_006E': 'local_government_m',
    'B24092_007E': 'state_government_m',
    'B24092_008E': 'federal_government_m',
    'B24092_009E': 'self_employed_not_inc_m',
    'B24092_010E': 'total_f',
    'B24092_011E': 'private_f',
    'B24092_012E': 'private_employee_f',
    'B24092_013E': 'private_self_employed_f',
    'B24092_014E': 'non_profit_f',
    'B24092_015E': 'local_government_f',
    'B24092_016E': 'state_government_f',
    'B24092_017E': 'federal_government_f',
    'B24092_018E': 'self_employed_not_inc_f'
}


# todo: at some point might want to include 3 and 4-digit naics codes
def NAICS_CODE_TO_ABB(digits, pub_admin=False):
    return pd.read_csv('https://www2.census.gov/programs-surveys/bds/technical-documentation/label_naics.csv') \
        .query(f'indlevel == {digits}') \
        .drop('indlevel', 1) \
        .query('name not in ["Public Administration", "Unclassified"]' if not pub_admin else '') \
        .set_index(['naics']) \
        .to_dict()['name']


BDS_VALID_CROSSES = [
    {'FAGE'}, {'EAGE'}, {'EMPSZFI'}, {'EMPSZES'}, {'EMPSZFII'}, {'EMPSZESI'},
    {'GEOCOMP', 'METRO'}, {'STATE'}, {'COUNTY'}, {'MSA'}, {'NAICS'}, 
    {'EMPSZFI', 'FAGE'}, {'EMPSZFII', 'FAGE'}, {'EAGE', 'EMPSZES'}, 
    {'EAGE', 'EMPSZESI'}, {'NAICS', 'STATE'}, {'GEOCOMP', 'METRO', 'STATE'},
    {'FAGE', 'STATE'}, {'EMPSZFI', 'STATE'}, {'EMPSZFII', 'STATE'}, 
    {'EAGE', 'STATE'}, {'EMPSZES', 'STATE'}, {'EMPSZESI', 'STATE'}, 
    {'FAGE', 'NAICS'}, {'EMPSZFI', 'NAICS'}, {'EMPSZFII', 'NAICS'},
    {'EAGE', 'NAICS'}, {'EMPSZES', 'NAICS'}, {'EMPSZESI', 'NAICS'},
    {'FAGE', 'GEOCOMP', 'METRO'}, {'EMPSZFI', 'GEOCOMP', 'METRO'},
    {'EMPSZFII', 'GEOCOMP', 'METRO'}, {'EAGE', 'GEOCOMP', 'METRO'},
    {'EMPSZES', 'GEOCOMP', 'METRO'}, {'EMPSZESI', 'GEOCOMP', 'METRO'}, 
    {'GEOCOMP', 'METRO', 'NAICS'}, {'FAGE', 'MSA'}, {'EMPSZFI', 'MSA'}, 
    {'EMPSZFII', 'MSA'}, {'EAGE', 'MSA'}, {'MSA', 'NAICS'}, {'COUNTY', 'FAGE'},
    {'COUNTY', 'EMPSZFI'}, {'COUNTY', 'EMPSZFII'}, {'COUNTY', 'EAGE'},
    {'COUNTY', 'NAICS'}, {'FAGE', 'MSA', 'NAICS'}, {'EMPSZFI', 'MSA', 'NAICS'},
    {'EMPSZFII', 'MSA', 'NAICS'}, {'EAGE', 'MSA', 'NAICS'}, 
    {'FAGE', 'NAICS', 'STATE'}, {'EMPSZFI', 'NAICS', 'STATE'}, 
    {'EMPSZFII', 'NAICS', 'STATE'}, {'EAGE', 'NAICS', 'STATE'},
    {'FAGE', 'GEOCOMP', 'METRO', 'NAICS'}, {'EMPSZFII', 'FAGE', 'NAICS'},
    {'EMPSZFI', 'GEOCOMP', 'METRO', 'NAICS'}, 
    {'EMPSZFII', 'GEOCOMP', 'METRO', 'NAICS'},
    {'EAGE', 'GEOCOMP', 'METRO', 'NAICS'}, {'FAGE', 'GEOCOMP', 'METRO', 'STATE'},
    {'EMPSZFI', 'GEOCOMP', 'METRO', 'STATE'}, 
    {'EMPSZFII', 'GEOCOMP', 'METRO', 'STATE'}, 
    {'EAGE', 'GEOCOMP', 'METRO', 'STATE'}, 
    {'GEOCOMP', 'METRO', 'NAICS', 'STATE'}, {'EMPSZFI', 'FAGE', 'NAICS'},
    {'FAGE', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'},
    {'EMPSZFI', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'}, 
    {'EMPSZFII', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'}, 
    {'EAGE', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'}
]