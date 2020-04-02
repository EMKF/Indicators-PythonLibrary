import sys
import requests
import pandas as pd
import kauffman_data.public_data_helpers

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:]

def get_data(series_lst, obs_level, start_year, end_year=None, seasonally_adj=True, annualize=True):
    """
    series_lst: lst; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf.
        net_job_creation
        fage4
        fsize
        age4 is not a valid variable

    obs_level: lst
        us:
        state:

    start_year:
        earliest year with fill compliment of age categories is 2003
    """

    if (not end_year) or (end_year > 2014):
        end_year = 2014
    elif end_year < start_year:
        end_year == start_year
    print('Using {startyear} and {endyear} as start and end years.'.format(startyear=start_year, endyear=end_year))

    print('Creating dataframe', end='...')
    df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        print(year, end='...')
        url = 'https://api.census.gov/data/timeseries/bds/firms?get={series_lst}&for={obs_level}:*&time={year}'.format(series_lst=','.join(series_lst), obs_level=obs_level, year=year)
        r = requests.get(url)
        df = df.append(
            pd.DataFrame(r.json()).\
                pipe(_make_header)
        )
    # todo: convert the outcome variables to int or float
    # todo: time should be a string? might need to sort
    return df


if __name__ == '__main__':
    df = get_data(['estabs', 'firms', 'fage4', 'fsize'], 'us', 2013).\
        astype({'estabs': 'int', 'firms': 'int'})
    print('\n')

    # df.pub.plot(['firms', 'estabs'], {'fage4': 'm', 'fsize': 'm'})
    df.pub.plot({'firms': 'Firms', 'estabs': 'Establishments'}, {'fage4': 'm', 'fsize': 'm'})