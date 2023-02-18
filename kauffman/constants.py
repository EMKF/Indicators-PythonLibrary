import pandas as pd
import geonamescache


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
        .drop(columns='CBSA Code') \
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

def get_msa_state_dict():
    df = pd.read_excel(
            'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
            skiprows=2, skipfooter=4,
            dtype = {'CBSA Code':'str', 'FIPS State Code':'str'}
        ) \
        .rename(columns={"CBSA Code":"fips", "FIPS State Code":"state_fips"}) \
        .drop_duplicates(['fips', 'state_fips']) \
        [['fips', 'state_fips']] \
        .pivot(values='state_fips', columns='fips')

    names = df.columns
    values = [list(df[col].dropna().values) for col in df]
    return dict(zip(names, values))

MSA_TO_STATE_FIPS = get_msa_state_dict()

STATE_TO_MSA_FIPS = {}
for k, v in MSA_TO_STATE_FIPS.items():
    for x in v:
        STATE_TO_MSA_FIPS.setdefault(x,[]).append(k)

KEY_WARN = 'WARNING: You did not provide a key. Too many requests will ' \
    'result in an error.'