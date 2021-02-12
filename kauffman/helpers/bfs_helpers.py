import sys
import datetime
import pandas as pd
from kauffman import constants as c
import numpy as np

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def raw_data_create(url):
    return pd.read_csv(url, skiprows=7).\
        dropna()


def _numerator_create(df, dur_in_lst):
    for q in dur_in_lst:
        df['numerator_sum{}'.format(q)] = df['BF_DUR{}Q'.format(q)] * df['BF_BF{}Q'.format(q)]
    return df


def _annual_filter_and_summer(x):
    if x.shape[0] == 4:  # filters based on number of observations in a given year...i.e., if we don't have four quarter's worth, throw it out.
        return x.iloc[:, 1:].sum()


def _annual_dur_create(df, dur_in_lst):
    for q in dur_in_lst:
        df.loc[:, 'BF_DUR{}Q'.format(q)] = df['numerator_sum{}'.format(q)] / df['BF_BF{}Q'.format(q)]
        df.drop(['numerator_sum{}'.format(q)], 1, inplace=True)
    return df


def _annualizer(df, annualize, dur_in_lst):
    if annualize:
        return df.\
                pipe(_numerator_create, dur_in_lst).\
                assign(Period=lambda x: x['Period'].str.split('-').str[1]).\
                groupby('Period').apply(_annual_filter_and_summer).\
                pipe(_annual_dur_create, dur_in_lst).\
                reset_index().\
                dropna().\
                rename(columns={0: 'Value'})
    return df


# todo: do I need this?
def remove_uneccesary_bfs(df, series_lst):
    print(df.head())
    sys.exit()
    return df[[col for col in df.columns if col in series_lst + ['Period']]]


def _date_formatter(df, annualize):
    if annualize:
        return df.assign(Period=lambda x: pd.to_datetime(x['Period']).dt.year)
    return df.assign(Period=pd.to_datetime(df['Period'], format='%b-%Y'))


def _features_create(df, region):
    df = df.\
        assign(
            region=region,
            fips=lambda x: x['region'].map(c.abb_fips_dic),
        ).\
        rename(columns={'Period': 'time'})


def _iso_year_start(iso_year):
    "The gregorian calendar date of the first day of the given ISO year"
    fourth_jan = datetime.date(iso_year, 1, 4)
    delta = datetime.timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta


def _iso_to_gregorian(iso_year, iso_week, iso_day):
    "Gregorian calendar date for the given ISO year, week and day"
    year_start = _iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day - 1, weeks=iso_week - 1)


def _bfs_data_create(region, series_lst, seasonally_adj, annualize):
    print(region)
    if annualize:
        dur_in_lst = list(map(lambda x: x[-2], [var for var in series_lst if 'DUR' in var]))
        bf_lst = ['BF_BF{}Q'.format(q) for q in dur_in_lst if 'BF_BF{}Q'.format(q) not in series_lst]
    else:
        dur_in_lst = []
        bf_lst = []

    df = pd.DataFrame(columns=['Period'])
    for series in series_lst + bf_lst:
        if 'DUR' in series:
            adjusted = 'no'
        else:
            if seasonally_adj:
                adjusted = 'yes'
            else:
                adjusted = 'no'

        url = 'https://www.census.gov/econ/currentdata/export/csv?programCode=BFS&timeSlotType=12&startYear=2004&endYear=2021&categoryCode=TOTAL&' + \
              f'dataTypeCode={series}&' + \
              f'geoLevelCode={region}&' + \
              f'adjusted={adjusted}&' + \
              'errorData=no&internal=false'

        df = pd.read_csv(url, skiprows=7).\
            rename(columns={'Value': series}).\
            merge(df, how='outer', on='Period')

    return df. \
        pipe(_date_formatter, annualize). \
        assign(
            region=region,
            fips=lambda x: x['region'].map(c.abb_fips_dic),
        ). \
        rename(columns={'Period': 'time'})

    # return df. \
    #     pipe(_annualizer, annualize, dur_in_lst). \
    #     pipe(_date_formatter, annualize).\
    #     pipe(_features_create, region)
#         pipe(remove_uneccesary_bfs, series_lst). \


if __name__ == '__main__':
    df = get_data(['BA_BA'], 'state', 2004, annualize=False)
    print(df)

    sys.exit()
    df = get_data('weekly', 'us', start_year=2004, annualize=False)
    print(df)
    sys.exit()



    # df = get_data(['BA_BA'], 'us', 2004, annualize=True)
    # df = get_data(['BF_DUR4Q', 'BF_DUR8Q', 'BA_BA'], 'state', 2004, annualize=True)

    # df = get_data(['BF_DUR4Q', 'BA_BA', 'BF_BF8Q'], 'state', 2004, annualize=False)
    # df = get_data(['BF_DUR4Q', 'BA_BA', 'BF_BF8Q'], 'us', 2004, annualize=False)

    df = get_data('weekly', 'us', start_year=2004, annualize=False)
    print(df.head())
    df = get_data('weekly', 'state', start_year=2004, annualize=False)
    print(df.info())
    print(df.head())
    print(df.tail())


# todo: https://www.census.gov/econ/bfs/csv/bfs_us_apps_weekly_nsa.csv
# todo: from https://www.census.gov/econ/bfs/index.html?#
# todo: dictionary: https://www.census.gov/econ/bfs/pdf/bfs_weekly_data_dictionary.pdf