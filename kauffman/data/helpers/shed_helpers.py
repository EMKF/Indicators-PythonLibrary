# data was downloaded from: https://www.federalreserve.gov/consumerscommunities/shed_data.htm


import io
import pandas as pd
from zipfile import ZipFile
import requests
import sys

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
# pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None


# 11. Maine
# 12. New Hampshire
# 13. Vermont
# 14. Massachusetts
# 15. Rhode Island
# 16. Connecticut
# 21. New York
# 22. New Jersey
# 23. Pennsylvania
# 31. Ohio
# 32. Indiana
# 33. Illinois
# 34. Michigan
# 35. Wisconsin
# 41. Minnesota
# 42. Iowa
# 43. Missouri
# 44. North Dakota
# 45. South Dakota
# 46. Nebraska
# 47. Kansas
# 51. Delaware
# 52. Maryland
# 53. District Of Columbia
# 54. Virginia
# 55. West Virginia
# 56. North Carolina
# 57. South Carolina
# 58. Georgia
# 59. Florida
# 61. Kentucky
# 62. Tennessee
# 63. Alabama
# 64. Mississippi
# 71. Arkansas
# 72. Louisiana
# 73. Oklahoma
# 74. Texas
# 81. Montana
# 82. Idaho
# 83. Wyoming
# 84. Colorado
# 85. New Mexico
# 86. Arizona
# 87. Utah
# 88. Nevada
# 91. Washington
# 92. Oregon
# 93. California
# 94. Alaska
# 95. Hawaii


# create a function to subset and rename columns
def column_formater(df):
    # remove non numeric characters from the time column
    df['time'] = df['time'].str.extract('(\d+)', expand=False).astype(int)
    # rename columns to lower case
    df.columns = df.columns.str.lower()
    # subset each df by columns of interest
    df = df[['caseid', 'time', 'ppstaten']]
    # replace values with state abbreviation
    df["ppstaten"].replace({31: "OH", 44: "ND", 59: "FL", 85: "NM"}, inplace=True)
    print(df.head())
    return df

# read in ba and wba
def _shed_data_create():
    # pull data for Phase One, Two, and Three by looping over each url parameter
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
    # shed_links = {
    #     'data_2013_(CSV)': 'SHED_public_use_data_2013.csv',
    # }
    df = pd.DataFrame()
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
    # df = plotter_1(df)

sys.exit()

# pull data for Phase One, Two, and Three by looping over each url parameter
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
# shed_links = {
#     'data_2013_(CSV)': 'SHED_public_use_data_2013.csv',
# }
df = pd.DataFrame()
for key, value in shed_links.items():
# # pull in each df by looping over url for each shed year
# z = urlopen('https://www.federalreserve.gov/consumerscommunities/files/SHED_' + str(key) + '.zip')
# # How can I get this line to stop saving the .csv in this python folder???
# myzip = ZipFile(BytesIO(z.read())).extract(value)
# # Why doesn't this remove the file?
# # os.remove(value)
# temp_df = pd.read_csv(myzip, low_memory=False, encoding='cp1252')