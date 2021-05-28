# data was downloaded from: https://www.federalreserve.gov/consumerscommunities/shed_data.htm


import io
import pandas as pd
from zipfile import ZipFile
import requests
import sys
from kauffman.tools.etl import read_zip


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None


# issues to resolve:
# 1) How to handle different questions and/or answer choices across similar variables in each year? e.g. d2
### For now,
# 2) How to clean up this script for kcr library?
# 3) What weights do we want to use?
# 4) How to integrate this into the kcr library?
# 5) Should someone (Mei?) take the time to go through each year's codebook and


# create a function to subset and rename columns
def column_formater(df):
    # remove non numeric characters from the time column
    df['time'] = df['time'].str.extract('(\d+)', expand=False).astype(int)
    # rename columns to lower case
    df.columns = df.columns.str.lower()
    # subset each df by columns of interest
    df = df[['caseid', 'time', 'ppstaten', 'e2']]
    # replace values with state abbreviation
    df["ppstaten"].replace({11: "ME", 12: "NH", 13: "VT", 14: "MA", 15: "RI", 16: "CT",
                            21: "NY", 22: "NJ", 23: "PA", 31: "OH", 32: "IN", 33: "IL", 34: "MI", 35: "WI",
                            41: "MN", 42: "IA", 43: "MO", 44: "ND", 45: "SD", 46: "NE", 47: "KS",
                            51: "DE", 52: "MD", 53: "DC", 54: "VA", 55: "WV", 56: "NC", 57: "SC", 58: "GA",
                            59: "FL", 61: "KY", 62: "TN", 63: "AL", 64: "MS", 71: "AR", 72: "LA", 73: "OK",
                            74: "TX", 81: "MT", 82: "ID", 83: "WY", 84: "CO", 85: "NM", 86: "AZ", 87: "UT",
                            88: "NV", 91: "WA", 92: "OR", 93: "CA", 94: "AK", 95: "HI"}, inplace=True)
    # make ppstaten upppercase
    # df['ppstaten'] = df['ppstaten'].str.lower()
    # replace values with answers
    df["e2"].replace({-1: "refused", 0: "no", 1: "yes"}, inplace=True)
    # make everything lowercase
    df = df.apply(lambda x: x.astype(str).str.lower())
    # Rename columns
    df.rename(columns={"caseid": "id", "ppstaten": "region", "e2": "med_exp_12_months"}, inplace=True)
    print(df.head())
    return df

# read in shed
def _shed_data_create():
    # pull data for Phase One, Two, and Three by looping over each url parameter
    # dict of dicts
    # {2013: {'url': url, name_dic: {}}}
    shed_links = {
        'data_2013_(CSV)': 'SHED_public_use_data_2013.csv',
        'public_use_data_2014_(CSV)': 'SHED_public_use_data_2014_update (occupation industry).csv',
        'public_use_data_2015_(CSV)': 'SHED 2015 public use.csv',
        'public_use_data_2016_(CSV)': 'SHED_2016_Public_Data.csv',
        'public_use_data_2017_(CSV)': 'SHED_2017_Public_Use.csv',
        'public_use_data_2018_(CSV)': 'public2018.csv',
        'public_use_data_2019_(CSV)': 'public2019.csv',
        'public_use_data_2020_(CSV)': 'public2020.csv',
    }
    # create df to append to
    df = pd.DataFrame()
    # loop over dict of urls and file names and append to df
    for key, value in shed_links.items():
        url = 'https://www.federalreserve.gov/consumerscommunities/files/SHED_' + str(key) + '.zip'
        r = requests.get(url)
        z = ZipFile(io.BytesIO(r.content))
        temp_df = pd.read_csv(z.open(value), low_memory=False, encoding='cp1252')
        # add a column for time
        temp_df['time'] = value
        # call another function here, within the for loop, to rename the column
        temp_df = column_formater(temp_df)
        # append each year of shed to df
        df = df.append(temp_df)
    print(df.head())
    df.to_csv('/Users/hmurray/Desktop/data/SHED/may_2021/data_create/shed_aggregated_5.21.csv')
    return df


if __name__ == '__main__':
    df = _shed_data_create()

sys.exit()

