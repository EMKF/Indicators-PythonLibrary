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
    return df.assign(region=region)


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


def get_data(series_lst, obs_level, start_year, end_year=None, seasonally_adj=True, annualize=True):
    """
    series_lst: lst
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

    obs_level: lst
        us:
        state: all of c.states
        any of c.states

    start_year:
        earliest year is 2004
    """
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

    df = get_data(['BF_DUR4Q', 'BA_BA', 'BF_BF8Q'], 'state', 2004, annualize=False)
    # df = get_data(['BF_DUR4Q', 'BA_BA', 'BF_BF8Q'], 'us', 2004, annualize=True)
    print(df)

