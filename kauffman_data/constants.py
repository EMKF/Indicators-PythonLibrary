import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
def filenamer(path):
    return os.path.join(ROOT_DIR, path)

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

bfs_series = ['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q']
bfs_series_brief = ['BF_SBF4Q', 'BF_DUR4Q']

state_abb_fips_dic = {
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
state_fips_abb_dic = {v: k for k, v in state_abb_fips_dic.items()}

us_state_abbrev = {
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

abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))

msa_fips_state_fips_dic = {
    '12060': '13', '12420': '48', '12580': '24', '13820': '01', '14460': '33', '15380': '36', '16740': '37', '16980': '55',
    '17140': '18', '17460': '39', '18140': '39', '19100': '48', '19740': '08', '19820': '26', '25540': '09', '26420': '48',
    '26900': '18', '27260': '12', '28140': '29', '29820': '32', '31080': '06', '31140': '18', '32820': '47', '33100': '12',
    '33340': '55', '33460': '55', '34980': '47', '35380': '22', '35620': '34', '36420': '40', '36740': '12', '37980': '10',
    '38060': '04', '38300': '42', '38900': '53', '39300': '25', '39580': '37', '40060': '51', '40140': '06', '40900': '06',
    '41180': '17', '41620': '49', '41700': '48', '41740': '06', '41860': '06', '41940': '06', '42660': '53', '45300': '12',
    '47260': '37', '47900': '11'
}
state_fips_msa_fips_dic = {v: k for k, v in msa_fips_state_fips_dic.items()}

indicator_dict = {
    'state': '',
    'bf_per_ba': 'NEW-EMPLOYER BUSINESS ACTUALIZATION',
    'bf_per_capita': 'RATE OF NEW-EMPLOYER BUSINESS',
    'avg_speed': 'NEW-EMPLOYER BUSINESS VELOCITY',
    'bf_per_firm': 'EMPLOYER BUSINESS NEWNESS',
    'index_geo': 'NEBAS INDEX',
}

ase_url_dict = {
    'Sector, Gender, Ethnicity, Race, and Veteran Status': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA01.zip?#',
    'Sector, Gender, Ethnicity, Race, Veteran Status, and Year in Business': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA02.zip?#',
    'Sector, Gender, Ethnicity, Race, Veteran Status, and Receipts Size of Firm': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA03.zip?#',
    'Sector, Gender, Ethnicity, Race, Veteran Status, and Employment Size of Firm': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA04.zip?#',
    'Number of Owners in Business': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSCB01.zip?#',
    'Majority of Business Family-Owned': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSCB02.zip?#'
}

lfs_series_dict = {
    'inc_self_employment': 'LNU02048984',
    'uninc_self_employment': 'LNU02027714',
    'civilian_labor_force': 'LNS11000000',
    'unemployment_rate': 'LNS14000000'
}
