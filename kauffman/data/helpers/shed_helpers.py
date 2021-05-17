# data was downloaded from: https://www.federalreserve.gov/consumerscommunities/shed_data.htm

import pandas as pd
import numpy as np
from zipfile import ZipFile
from urllib.request import urlopen
from io import BytesIO
import sys

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
# pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None

# read in ba and wba
def data_create():
    # pull data for Phase One, Two, and Three by looping over each url parameter
    shed_links = {
        'data_2013_(CSV)': 'SHED_public_use_data_2013.csv',
        'public_use_data_2014_(CSV)': 'SHED_public_use_data_2014_update (occupation industry).csv',
        'public_use_data_2015_(CSV)': 'SHED 2015 public use.csv',
        'public_use_data_2016_(CSV)': 'SHED_2016_Public_Data.csv',
        'public_use_data_2017_(CSV)': 'SHED_2017_Public_Use.csv',
        'public_use_data_2018_(CSV)': 'public2018.csv',
        'public_use_data_2019_(CSV)': 'public2019.csv',
    }
    df = pd.DataFrame()
    for key, value in shed_links.items():
        # pull in each df by looping over url for each shed year
        z = urlopen('https://www.federalreserve.gov/consumerscommunities/files/SHED_' + str(key) + '.zip')
        # How can I get this line to stop saving the .csv in this python folder???
        myzip = ZipFile(BytesIO(z.read())).extract(value)
        shed_year = pd.read_csv(myzip, low_memory=False, encoding='cp1252')
        print(shed_year.head())
    # append each year of shed to df
    df = shed_year.append(shed_year, ignore_index=True)
    print(df.head())
    df.to_csv('/Users/hmurray/Desktop/data/SHED/may_2021/data_create/shed_aggregated_5.21.csv')
    return df


if __name__ == '__main__':
    df = data_create()
    # df = plotter_1(df)

sys.exit()