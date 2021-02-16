import requests
import pandas as pd

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _fetch_data(year, var_lst):
    if year == 2002:
        var_lst += ',NAICS2002,NAICS_TTL'



    url = 'https://api.census.gov/data/{year}/nonemp?get={var_lst}&for=us:*'.format(year=year, var_lst=','.join(var_lst))
    print('{0}: {1}'.format(year, url))
    print('https://api.census.gov/data/2017/nonemp?get=NESTAB,NAICS2017_TTL&for=us:*')
    'https://api.census.gov/data/2017/nonemp?get=NESTAB,NAICS2017_TTL,RCPSZES&for=us:*'
    r = requests.get(url)

    return pd.DataFrame(r.json())


def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:, :].reset_index(drop=True)


def _rename_etc(df, year):
    if year == 2002:
        df = df.query('NAICS2002 == "00"').drop(['NAICS2002', 'NAICS_TTL'], 1)
    return df.\
        assign(year=year).\
        rename(columns={'NRCPTOT': 'receipts', 'NESTAB': 'num_estab'})

def get_data(series_lst=['NESTAB'], obs_level='us', start_year=None, end_year=None, seasonally_adj=True, annualize=True):
    """
    series_lst: lst; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf.
        NESTAB: Number of nonemployer establishments. Available from 1997 through 2017.
        NRCPTOT: Receipts in thousands of dollars. Available from 1997 through 2017.
        LFO: Legal form of organization code. Available from 2008 through 2017.

    start_year:
        earliest year with fill compliment of age categories is 2003

    TODO
    obs_level: lst
        us:
        state:

    """
    ''
    if not start_year:
        start_year = 1997
    if not end_year:
        end_year = 2017

    print('Using {startyear} and {endyear} as start and end years.'.format(startyear=start_year, endyear=end_year))

    print('Creating dataframe', end='...')
    df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        print(year, end='...')
        url = 'https://api.census.gov/data/{year}/nonemp?get={series_lst}&for=us:*'.format(year=year, series_lst=','.join(series_lst))
        df_temp = pd.DataFrame(requests.get(url).json()). \
            pipe(_make_header).\
            drop('us', 1).\
            loc[:0].\
            assign(time=str(year))
        df = df.append(df_temp)
    return df.reset_index(drop=True)


if __name__ == '__main__':
    df = get_data(series_lst=['NESTAB', 'NRCPTOT'])

    df.\
        astype({'NESTAB': 'int', 'NRCPTOT': 'int'}).\
        assign(receipt_rate=lambda x: x['NRCPTOT'] / x['NESTAB'])

