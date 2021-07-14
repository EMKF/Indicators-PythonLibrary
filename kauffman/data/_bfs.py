import sys
import datetime
import pandas as pd
from kauffman import constants as c

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)




# url = f'https://api.census.gov/data/timeseries/eits/bfs?get=data_type_code,seasonally_adj,category_code,cell_value,error_data&for=us:*&time={year}'
def _url(region, series, seasonally_adj, industry):
    if 'DUR' in series:
        adjusted = 'no'
    else:
        if seasonally_adj:
            adjusted = 'yes'
        else:
            adjusted = 'no'
    return 'https://www.census.gov/econ/currentdata/export/csv?programCode=BFS&timeSlotType=12&startYear=2004&endYear=2021&' + \
          f'categoryCode={industry}&' + \
          f'dataTypeCode={series}&' + \
          f'geoLevelCode={region}&' + \
          f'adjusted={adjusted}&' + \
          'errorData=no&internal=false'


def _year_create_shift(x):
    if x['time'].month <= 3:
        return x['time'].year
    return x['time'].year + 1


def _time_annualize(df, march_shift):
    if march_shift:
        return df.assign(time=lambda x: x.apply(_year_create_shift, axis=1))
    return df.assign(time=lambda x: x['time'].dt.year)


def _DUR_numerator(df):
    if 'BF_DUR4Q' in df.columns:
        df = df.assign(DUR4_numerator=lambda x: x['BF_DUR4Q'] * x['BF_BF4Q'])
    if 'BF_DUR8Q' in df.columns:
        df = df.assign(DUR8_numerator=lambda x: x['BF_DUR8Q'] * x['BF_BF8Q'])
    return df


def _BF_DURQ(df):
    if 'BF_DUR4Q' in df.columns:
        df = df.\
            assign(BF_DUR4Q=lambda x: x['DUR4_numerator'] / x['BF_BF4Q']).\
            drop('DUR4_numerator', 1)
    if 'BF_DUR8Q' in df.columns:
        df = df.\
            assign(BF_DUR8Q=lambda x: x['DUR8_numerator'] / x['BF_BF8Q']). \
            drop('DUR8_numerator', 1)
    return df


def _annualize(df, annualize, bf_helper_lst, march_shift):
    if annualize:
        return df.\
            pipe(_time_annualize, march_shift). \
            pipe(_DUR_numerator). \
            groupby(['fips', 'region', 'time']).sum(min_count=12).\
            pipe(_BF_DURQ).\
            reset_index(drop=False) \
            [[col for col in df.columns if col not in bf_helper_lst]]
    return df


def _df_series(series_lst, bf_helper_lst, region, seasonally_adj, industry):
    df = pd.DataFrame(columns=['Period'])
    for series in series_lst + bf_helper_lst:
        df = pd.read_csv(_url(region, series, seasonally_adj, industry), skiprows=7). \
            rename(columns={'Value': series}). \
            merge(df, how='outer', on='Period')
    return df


def _bfs_data_create(region, series_lst, industry_lst, seasonally_adj, annualize, march_shift):
    # print(region, series_lst, industry_lst); sys.exit()
    if march_shift: annualize = True

    bf_helper_lst = []
    if annualize:
        if ('BF_DUR4Q' in series_lst) and ('BF_BF4Q' not in series_lst): bf_helper_lst.append('BF_BF4Q')
        if ('BF_DUR8Q' in series_lst) and ('BF_BF8Q' not in series_lst): bf_helper_lst.append('BF_BF8Q')

    return pd.concat(
            [
                _df_series(series_lst, bf_helper_lst, region, seasonally_adj, industry).\
                    assign(industry=industry)
                for industry in industry_lst
            ]
        ). \
        assign(
            time=lambda x: pd.to_datetime(x['Period'], format='%b-%Y'),
            region=c.state_abb_to_name[region],
            fips=lambda x: c.state_abb_to_fips[region],
        ). \
        drop('Period', 1). \
        pipe(_annualize, annualize, bf_helper_lst, march_shift) \
        [['fips', 'region', 'time', 'industry'] + series_lst]. \
        reset_index(drop=True)


def bfs(series_lst, obs_level='all', industry='Total', seasonally_adj=True, annualize=False, march_shift=False):
    """ Create a pandas data frame with results from a BFS query. Column order: fips, region, time, series_lst.


    Keyword arguments:
    series_lst-- lst of variables to be pulled.

        Variables:
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

    obs_level-- The level to pull observations for. ('state', 'us', or 'all')

    industry
        Variables:
            all: all industries and total
            Total: Sum across all industries
            NAICS11: Agriculture
            NAICS21: Mining
            NAICS22: Utilities
            NAICS23: Construction
            NAICSMNF: Manufacturing
            NAICS42: Wholesale Trade
            NAICSRET: Retail Trade
            NAICSTW: Transportation and Warehousing
            NAICS51: Information
            NAICS52: Finance and Insurance
            NAICS53: Real Estate
            NAICS54: Professional Services
            NAICS55: Management of Companies
            NAICS56: Administrative and Support
            NAICS61: Educational Services
            NAICS62: Health Care and Social Assistance
            NAICS71: Arts and Entertainment
            NAICS72: Accomodation and Food Services
            NAICS81: Other Services
            NONAICS: No NAICS Assigned


    seasonally_adj-- Option to use the census adjustment for seasonality and smooth the time series. (True or False)

    annualize-- Aggregates across months and annulizes data. (True or False)

    march_shift-- When True the year end is March, False the year end is December. (True or False)



    """

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level == 'us':
            region_lst = ['US']
        elif obs_level == 'state':
            region_lst = c.states
        else:
            region_lst = ['US'] + c.states

    if type(industry) == list:
        industry_lst = industry
    else:
        if industry == 'all':
            industry_lst = c.bfs_industries
        else:
            industry_lst = [industry.upper()]

    return pd.concat(
            [
                _bfs_data_create(region, series_lst, industry_lst, seasonally_adj, annualize, march_shift)
                for region in region_lst
            ],
            axis=0
        )


