import sys
import pandas as pd

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _fetch_crosswalk(year):
    """Extract the crosswalk files"""
    url = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/{0}/historical-delineation-files/list3.xls'.format(year)
    return pd.read_excel(url).\
        assign(year=year)


def _crosswalk_format(df):
    """Format the crosswalk tables pulled in the _fetch_crosswalk function"""
    col_row = df.loc[df.iloc[:, 0] == 'CBSA Code'].index.tolist()[0]
    df.columns = df.iloc[col_row, :].tolist()[:-1] + ['year']
    df = df.iloc[col_row + 1:] \
        [['year', 'CBSA Code', 'FIPS']]. \
        query('FIPS == FIPS'). \
        rename(columns={'FIPS': 'fips', 'year': 'time'}). \
        astype({'time': 'str', 'fips': 'str', 'CBSA Code': 'str'})
    df['CBSA Code'] = df['CBSA Code'].replace({'31100': '31080'})
    return df


def county_msa_cross_walk(year=2009):  # county to msa and visa versa
    """
    Creates a dataframe that allows msa to county (and visa versa) cross-walking for years between 2004 through 2009.
    Files for other years might be available. Consult
    https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/.
    """
    # todo: all of this.

    # todo: years prior to 2004 and after 2009
    return pd.concat(
            [
                _fetch_crosswalk(year). \
                    pipe(_crosswalk_format)
                for year in range(2004, 2010)
            ]
        ).\
        reset_index(drop=True).\
        rename(columns={'CBSA Code': 'msa_fips', 'fips': 'county_fips'})  #.\
        # query('time == "2009"')


if __name__ == '__main__':
    get_data()
