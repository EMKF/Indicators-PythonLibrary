import io
import sys
import pandas as pd
import requests
from zipfile import ZipFile


state_codes = {11: "ME", 12: "NH", 13: "VT", 14: "MA", 15: "RI", 16: "CT",
                            21: "NY", 22: "NJ", 23: "PA", 31: "OH", 32: "IN", 33: "IL", 34: "MI", 35: "WI",
                            41: "MN", 42: "IA", 43: "MO", 44: "ND", 45: "SD", 46: "NE", 47: "KS",
                            51: "DE", 52: "MD", 53: "DC", 54: "VA", 55: "WV", 56: "NC", 57: "SC", 58: "GA",
                            59: "FL", 61: "KY", 62: "TN", 63: "AL", 64: "MS", 71: "AR", 72: "LA", 73: "OK",
                            74: "TX", 81: "MT", 82: "ID", 83: "WY", 84: "CO", 85: "NM", 86: "AZ", 87: "UT",
                            88: "NV", 91: "WA", 92: "OR", 93: "CA", 94: "AK", 95: "HI"}



def read_zip(zip_url, filename):
    """
    Reads a csv file from a zip file online. Example: 'public2018.csv' from
    'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip'
    """
    z = ZipFile(io.BytesIO(requests.get(zip_url).content))
    return pd.read_csv(z.open(filename), low_memory=False, encoding='cp1252')


shed_dic = {
    2013: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_data_2013_(CSV).zip',
        'filename': 'SHED_public_use_data_2013.csv',
        'col_name_dic': {
            'v1_2013': 'v1_final',
            'v2_2013': 'v2_final',
            'v3_2013': 'v3_final',
            'v4_2013': 'v4_final',
        }
    },
    2014: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2014_(CSV).zip',
        'filename': 'SHED_public_use_data_2014_update (occupation industry).csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
        }
    },
    2015: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2015_(CSV).zip',
        'filename': 'SHED 2015 public use.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
        }
    },
    2016: {
        'zip_url': 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2016_(CSV).zip',
        'filename': 'SHED_2016_Public_Data.csv',
        'col_name_dic': {
            'v1_2014': 'v1_final',
            'v2_2014': 'v2_final',
            'v3_2014': 'v3_final',
            'v4_2014': 'v4_final',
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


shed_links = {

    'public_use_data_2015_(CSV)': 'SHED 2015 public use.csv',
    'public_use_data_2016_(CSV)': 'SHED_2016_Public_Data.csv',
    'public_use_data_2017_(CSV)': 'SHED_2017_Public_Use.csv',
    'public_use_data_2018_(CSV)': 'public2018.csv',
    'public_use_data_2019_(CSV)': 'public2019.csv',
    'public_use_data_2020_(CSV)': 'public2020.csv',
}
