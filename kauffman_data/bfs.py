import sys
import datetime
import pandas as pd
from kauffman_data import constants as c
import kauffman_data.public_data_helpers

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


def annualizer(df, annualize, dur_in_lst):
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


def remove_uneccesary_bfs(df, series_lst):
    return df[[col for col in df.columns if col in series_lst + ['Period']]]


def date_formatter(df, annualize):
    if annualize:
        return df.assign(Period=lambda x: pd.to_datetime(x['Period']).dt.year)
    df['Period'] = df.apply(lambda x: pd.to_datetime(x['Period'].split('-')[1]).replace(month=3 * int(x['Period'].split('-')[0][-1])), axis=1).dt.to_period('Q')
    return df


def features_create(df, region):
    return df.\
        assign(region=region).\
        rename(columns={'Period': 'time'})


def region_data_frame_create(region, series_lst, seasonally_adj, start_year, end_year, annualize):
    if annualize:
        dur_in_lst = list(map(lambda x: x[-2], [var for var in series_lst if 'DUR' in var]))
        bf_lst = ['BF_BF{}Q'.format(q) for q in dur_in_lst if 'BF_BF{}Q'.format(q) not in series_lst]
    else:
        dur_in_lst = []
        bf_lst = []

    df = pd.DataFrame(columns=['Period'])
    for series in series_lst + bf_lst:
        data_type_code = 'T'

        if 'DUR' in series:
            adjusted = 'no'
            data_type_code = 'Q'
        else:
            if seasonally_adj:
                adjusted = 'yes'
            else:
                adjusted = 'no'

        url = 'https://www.census.gov/econ/currentdata/export/csv?programCode=BFS&timeSlotType=4&' + \
              'startYear={start_year}&endYear={end_year}&'.format(start_year=start_year, end_year=end_year) + \
              'categoryCode={series}'.format(series=series) + \
              '&dataTypeCode={data_type_code}&'.format(data_type_code=data_type_code) + \
              'geoLevelCode={obs_level}&'.format(obs_level=region) + \
              'adjusted={adjusted}&'.format(adjusted=adjusted) + \
              'errorData=no&internal=false'

        df = raw_data_create(url).\
            rename(columns={'Value': series}).\
            merge(df, how='outer', on='Period')

    return df. \
        pipe(annualizer, annualize, dur_in_lst). \
        pipe(remove_uneccesary_bfs, series_lst). \
        pipe(date_formatter, annualize).\
        pipe(features_create, region)


# import datetime
# datetime.date(2020, 12, 28).isocalendar()
#
# datetime.date.fromisocalendar(2020, 53, 0)
#
# pd.DateOffset(weeks=t) + pd.to_datetime('2006-01-07'))

def _iso_year_start(iso_year):
    "The gregorian calendar date of the first day of the given ISO year"
    fourth_jan = datetime.date(iso_year, 1, 4)
    delta = datetime.timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta

def _iso_to_gregorian(iso_year, iso_week, iso_day):
    "Gregorian calendar date for the given ISO year, week and day"
    year_start = _iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day - 1, weeks=iso_week - 1)

def get_data(series_lst, obs_level='us', start_year=None, end_year=None, seasonally_adj=True, annualize=True):
    """
    series_lst: lst or 'weekly'
        If quarterly data, then the list must consist of one of the following variable names.

        Quarterly Variables:
            BA_BA: 'Business Applications'
            BA_CBA: 'Business Applications from Corporations'
            BA_HBA: 'High-Propensity Business Applications'
            BA_WBA: 'Business Applications with Planned Wages'
            BF_BF4Q: 'Business Formations within Four Quarters'
            BF_BF8Q: 'Business Formations within Eight Quarters'
            BF_PBF4Q: Projected Business Formations within Four Quarters
            BF_PBF8Q: Projected Business Formations within Eight Quarters
            BF_SBF4Q: Spliced Business Formations within Four Quarter
            BF_SBF8Q: Spliced Business Formations within Eight Quarters
            BF_DUR4Q: Average Duration (in Quarters) from Business Application to Formation within Four Quarters
            BF_DUR8Q: Average Duration (in Quarters) from Business Application to Formation within Eight Quarters

        Alternatively, if 'weekly' then the entire weekly dataset is returned for the corresponding observation level.

        Weekly Variables:
            BA_NSA: Not seasonally adjusted Business Application national series
            HBA_BSA: Not seasonally adjusted High-Propensity Business Application national series
            WBA_NSA: Not seasonall adjusted Businesses Applications with Planned Wages national series
            CBA_NSA: Not seasonally adjusted Business Applications from Corporations national series
            YY_BA_NSA: Calculated year‐to‐year percentage changes for same week a year ago for the Business Application
                series. No values are available for first year of the series (2006) or for week 53s.
            YY_HBA_NSA: Calculated year‐to‐year percentage changes for same week a year ago for the High‐Propensity
                Business Application series. No values are available for first year of the series (2006) or for week
                53s.
            YY_WBA_NSA: Calculated year‐to‐year percentage changes for same week a year ago for the Businesses
                Applications with Planned Wages series. No values are available for first year of the series (2006) or
                for week 53s.
            YY_CBA_NSA: Calculated year‐to‐year percentage changes for same week a year ago for the Businesses
                Applications with Planned Wages series. No values are available for first year of the series (2006) or
                for week 53s.

    obs_level: lst
        us:
        state: all of c.states
        any of c.states

    start_year:
        Earliest year is 2004
    """

    # todo: get this better integrated into the code
    if isinstance(series_lst, str):
        return pd.read_csv('https://www.census.gov/econ/bfs/csv/bfs_{obs_level}_apps_weekly_nsa.csv'.format(obs_level=obs_level)). \
            assign(region=lambda x: x['State'] if obs_level == 'state' else 'US'). \
            assign(
                time=lambda x: x.apply(lambda t: _iso_to_gregorian(int(t['Year']), int(t['Week']), 6), axis=1)
            ).\
            astype({'time': 'str'}).\
            drop(['Year', 'Week', 'State'] if obs_level == 'state' else ['Year', 'Week'], 1)

    if not start_year:
        start_year = 2004
    if not end_year:
        end_year = datetime.datetime.now().year

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level == 'state':
            region_lst = c.states
        else:
            region_lst = [obs_level]

    print('Collecting data for...')
    df = pd.DataFrame()
    for region in region_lst:
        print(region)
        df = df.append(region_data_frame_create(region, series_lst, seasonally_adj, start_year, end_year, annualize))
    return df.reset_index(drop=True)


if __name__ == '__main__':
    # df = get_data(['BA_BA'], 'state', 2004, annualize=True)
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