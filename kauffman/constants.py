import pandas as pd
import geonamescache
from itertools import product
import os

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

state_abb_to_fips = {
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

state_fips_to_abb = {v: k for k, v in state_abb_to_fips.items()}

state_name_to_abb = {
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

state_abb_to_name = dict(map(reversed, state_name_to_abb.items()))

def msa_fips_to_name():
    return pd.read_excel(
        'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
        header=2,
        skipfooter=4,
        usecols=[0, 3],
    ). \
        assign(fips=lambda x: x['CBSA Code'].astype(str)). \
        drop('CBSA Code', 1).\
        drop_duplicates('fips').\
        set_index(['fips']).\
        to_dict()['CBSA Title']


# todo: can I do this with the first of these lines? I don't know what genomaescache has. Given I import it, I might as well use it if I can.
all_fips_to_name = {
    **{'01001': 'Autauga County'},
    **{dict['fips']: dict['name'] for dict in geonamescache.GeonamesCache().get_us_counties()},
    **msa_fips_to_name(),
    **{k: state_abb_to_name[v] for k, v in state_fips_to_abb.items() if v != 'PR'}
}
all_name_to_fips = dict(map(reversed, all_fips_to_name.items()))

# todo: put these in functions so don't always call unless needed
def qwi_start_to_end_year():
    return pd.read_html('https://ledextract.ces.census.gov/loading_status.html')[0] \
        [['State', 'Start Quarter', 'End Quarter']].\
        assign(
            start_year=lambda x: x['Start Quarter'].str.split().str[0],
            end_year=lambda x: x['End Quarter'].str.split().str[0],
            fips=lambda x: x['State'].map(state_abb_to_fips),
        ).\
        set_index('fips') \
        [['start_year', 'end_year']].\
        to_dict('index')


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
def filenamer(path):
    return os.path.join(ROOT_DIR, path)

msa_to_state_fips = {
    '12060': ['13'],
    '12420': ['48'],
    '12580': ['24'],
    '13820': ['01'],
    '15380': ['36'],
    '17460': ['39'],
    '18140': ['39'],
    '19100': ['48'],
    '19740': ['08'],
    '19820': ['26'],
    '25540': ['09'],
    '26420': ['48'],
    '26900': ['18'],
    '27260': ['12'],
    '29820': ['32'],
    '31080': ['06'],
    '33100': ['12'],
    '33340': ['55'],
    '34980': ['47'],
    '35380': ['22'],
    '36420': ['40'],
    '36740': ['12'],
    '38060': ['04'],
    '38300': ['42'],
    '39580': ['37'],
    '40060': ['51'],
    '40140': ['06'],
    '40900': ['06'],
    '41620': ['49'],
    '41700': ['48'],
    '41740': ['06'],
    '41860': ['06'],
    '41940': ['06'],
    '42660': ['53'],
    '45300': ['12'],
    '47900': ['11', '24', '54', '51'],
    '47260': ['37', '51'],
    '41180': ['17', '29'],
    '38900': ['41', '53'],
    '35620': ['34', '36', '42'],
    '14460': ['25', '33'],
    '16740': ['37', '45'],
    '16980': ['17', '18', '55'],
    '17140': ['18', '21', '39'],
    '28140': ['20', '29'],
    '31140': ['18', '21'],
    '32820': ['05', '28', '47'],
    '33460': ['27', '55'],
    '37980': ['24', '34', '10', '42'],
    '39300': ['25', '44'],
}
# msa_fips_state_fips_dic = {
#     '12060': '13', '12420': '48', '12580': '24', '13820': '01', '14460': '33', '15380': '36', '16740': '37', '16980': '55',
#     '17140': '18', '17460': '39', '18140': '39', '19100': '48', '19740': '08', '19820': '26', '25540': '09', '26420': '48',
#     '26900': '18', '27260': '12', '28140': '29', '29820': '32', '31080': '06', '31140': '18', '32820': '47', '33100': '12',
#     '33340': '55', '33460': '55', '34980': '47', '35380': '22', '35620': '34', '36420': '40', '36740': '12', '37980': '10',
#     '38060': '04', '38300': '42', '38900': '53', '39300': '25', '39580': '37', '40060': '51', '40140': '06', '40900': '06',
#     '41180': '17', '41620': '49', '41700': '48', '41740': '06', '41860': '06', '41940': '06', '42660': '53', '45300': '12',
#     '47260': '37', '47900': '11'
# }
# state_fips_msa_fips_dic = {v: k for k, v in msa_to_state_fips.items()}

age_code_to_label = {
    0: 'Less than one year old',
    1: '1 to 4 years',
    2: '5 to 9 years',
    3: '10 years or older',
    4: 'All'
}

size_code_to_label = {
    0: '1 to 4 employees',
    1: '5 to 9 employees',
    2: '10 to 19 employees',
    3: '20 to 49 employees',
    4: '50 to 99 employees',
    5: '100 to 499 employees',
    6: '500 or more employees',
    7: 'All'
}
age_size_lst = list(product(age_code_to_label.keys(), size_code_to_label.keys()))

table1bf_cols = ['time', 'firms', 'establishments', 'net_change', 'total_job_gains', 'expanding_job_gains',
                    'opening_job_gains', 'total_job_losses', 'contracting_job_losses', 'closing_job_losses']

table_firm_size_cols = ['time', 'quarter', 'net_change', 'total_job_gains', 'expanding_firms_job_gains',
             'opening_firms_job_gains', 'total_job_losses', 'contracting_firms_job_losses',
             'closing_firms_job_losses']

size_code_to_label2 = {
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

month_to_quarter = {
    'March': 1,
    'June': 2,
    'September': 3,
    'December': 4
}

qwi_outcomes = [
    'EarnBeg', 'EarnHirAS', 'EarnHirNS', 'EarnS', 'EarnSepS', 'Emp', 'EmpEnd', 'EmpS', 'EmpSpv', 'EmpTotal', 'FrmJbC',
    'FrmJbCS', 'FrmJbGn', 'FrmJbGnS', 'FrmJbLs', 'FrmJbLsS', 'HirA', 'HirAEnd', 'HirAEndR', 'HirAEndRepl',
    'HirAEndReplr', 'HirAs', 'HirN', 'HirNs', 'HirR', 'Payroll', 'Sep', 'SepBeg', 'SepBegR', 'SepS', 'SepSnx',
    'TurnOvrS'
]

qwi_averaged_outcomes = ['EarnS', 'EarnBeg', 'EarnHirAS', 'EarnHirNS', 'EarnSepS']

acs_code_to_var = {
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


# todo: new constants to vet

shed_dic = {
    2013: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_data_2013_(CSV).zip',
        'filename': 'SHED_public_use_data_2013.csv',
        'col_name_dic': {
            'v1_2013': 'v1_final',  # todo: at some point in the future there might be variables to rename...use as rename(c.shed_dic[year]['col_name_dic'])
        }
    },
    2014: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2014_(CSV).zip',
        'filename': 'SHED_public_use_data_2014_update (occupation industry).csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
        }
    },
    2015: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2015_(CSV).zip',
        'filename': 'SHED 2015 public use.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
        }
    },
    2016: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2016_(CSV).zip',
        'filename': 'SHED_2016_Public_Data.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
        }
    },
    2017: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2017_(CSV).zip',
        'filename': 'SHED_2017_Public_Use.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
        }
    },
    2018: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip',
        'filename': 'public2018.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
        }
    },
    2019: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2019_(CSV).zip',
        'filename': 'public2019.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
        }
    },
    2020: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2020_(CSV).zip',
        'filename': 'public2020.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
        }
    },
}

state_shed_codes_to_abb = {11: "ME", 12: "NH", 13: "VT", 14: "MA", 15: "RI", 16: "CT",
                            21: "NY", 22: "NJ", 23: "PA", 31: "OH", 32: "IN", 33: "IL", 34: "MI", 35: "WI",
                            41: "MN", 42: "IA", 43: "MO", 44: "ND", 45: "SD", 46: "NE", 47: "KS",
                            51: "DE", 52: "MD", 53: "DC", 54: "VA", 55: "WV", 56: "NC", 57: "SC", 58: "GA",
                            59: "FL", 61: "KY", 62: "TN", 63: "AL", 64: "MS", 71: "AR", 72: "LA", 73: "OK",
                            74: "TX", 81: "MT", 82: "ID", 83: "WY", 84: "CO", 85: "NM", 86: "AZ", 87: "UT",
                            88: "NV", 91: "WA", 92: "OR", 93: "CA", 94: "AK", 95: "HI"}


recession_dates = pd.read_html('https://www.nber.org/research/data/us-business-cycle-expansions-and-contractions')[0] \
    ['Business Cycle Reference Dates'].\
    loc[lambda x: x['Peak Month'] != '-'].\
    loc[lambda x: x['Trough Month'] != '-'].\
    assign(
        peak_time=lambda x: pd.to_datetime(x['Peak Year'].astype(str) + x['Peak Month'], format='%Y%B'),
        trough_time=lambda x: pd.to_datetime(x['Trough Year'].astype(str) + x['Trough Month'], format='%Y%B'),
    ) \
    [['peak_time', 'trough_time']].\
    values

bfs_industries = [
    'NAICS11', 'NAICS21', 'NAICS22', 'NAICS23', 'NAICS42', 'NAICS51', 'NAICS52',
    'NAICS53', 'NAICS54', 'NAICS55', 'NAICS56', 'NAICS61', 'NAICS62', 'NAICS71',
    'NAICS72', 'NAICS81', 'NAICSMNF', 'NAICSRET', 'NAICSTW', 'NONAICS', 'TOTAL'
]

naics_to_bfsnaics = {
    '00': 'TOTAL',
    '11': 'NAICS11',
    '21': 'NAICS21',
    '22': 'NAICS22',
    '23': 'NAICS23',
    '31-33': 'NAICSMNF',
    '42': 'NAICS42',
    '44-45': 'NAICSRET',
    '48-49': 'NAICSTW',
    '51': 'NAICS51',
    '52': 'NAICS52',
    '53': 'NAICS53',
    '54': 'NAICS54',
    '55': 'NAICS55',
    '56': 'NAICS56',
    '61': 'NAICS61',
    '62': 'NAICS62',
    '71': 'NAICS71',
    '72': 'NAICS72',
    '81': 'NAICS81',
    'ZZ': 'NONAICS'
}

# todo: at some point might want to include 3 and 4-digit naics codes
def naics_code_to_abb(digits, pub_admin=False):
    return pd.read_csv('https://www2.census.gov/programs-surveys/bds/technical-documentation/label_naics.csv').\
        query(f'indlevel == {digits}').\
        drop('indlevel', 1).\
        query('name not in ["Public Administration", "Unclassified"]' if not pub_admin else '').\
        set_index(['naics']).\
        to_dict()['name']
