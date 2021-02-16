import pandas as pd
from kauffman import constants as c
from kauffman import data_manager as dm

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


# Hayden test 123 Hayden test 123
def geofilterer(df):
    return df.query('GEO_TTL=="United States"')

def get_data(series_name, year):
    """
    series_name: str
        Company Summary:
            'Sector, Gender, Ethnicity, Race, and Veteran Status': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA01.zip?#'
            'Sector, Gender, Ethnicity, Race, Veteran Status, and Year in Business': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA02.zip?#'
            'Sector, Gender, Ethnicity, Race, Veteran Status, and Receipts Size of Firm': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA03.zip?#'
            'Sector, Gender, Ethnicity, Race, Veteran Status, and Employment Size of Firm': 'https://www2.census.gov/econ20{year}/SE/sector00/SE{year}00CSA04.zip?#'

        Characteristics of Businesses:
            'Number of Owners in Business': 'https://www2.census.gov/econ2016/SE/sector00/SE1600CSCB01.zip?#'
            'Majority of Business Family-Owned': 'https://www2.census.gov/econ2016/SE/sector00/SE1600CSCB02.zip?#'

    year: int
        2014, 2015, or 2016
    """

    return dm.zip_to_dataframe(c.ase_url_dict[series_name].format(year=year - 2000))


if __name__ == '__main__':
    # df = get_data('Sector, Gender, Ethnicity, Race, and Veteran Status', 2014)
    df = get_data('Sector, Gender, Ethnicity, Race, and Veteran Status', 2016)
    print(df.head())
