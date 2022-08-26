import pandas as pd
import numpy as np
from kauffman import constants as c
from zipfile import ZipFile
from io import BytesIO
import urllib.request as urllib2


def _format_df(df):
    df.columns = df.iloc[1]
    return df.dropna(axis=1, how='all') \
        .iloc[2:]

	
def _fetch_data():
    link = 'https://www.census.gov/econ/currentdata/clutch/getzippedfile?program=BFS&filename=BFS-mf.zip'
    r = urllib2.urlopen(link).read()
    file = ZipFile(BytesIO(r)) 
    bfs_file = file.open("BFS-mf.csv")

    df = pd.read_csv(
        bfs_file, names=['a', 'b', 'c', 'd', 'e', 'f'], dtype='str'
    )
    names = [
        'CATEGORIES', 'DATA TYPES', 'GEO LEVELS', 'TIME PERIODS', 'NOTES',
        'DATA'
    ]
    row_splits = list(df.query(f'a in {names}').index)

    industry_key, series_key, region_key, time_key = [
        df[row_splits[i]:row_splits[i+1]] \
            .pipe(_format_df)
        for i in range(0,4)
    ]
    data = _format_df(df[row_splits[-1]:])

    return data, industry_key, series_key, region_key, time_key
	

def clean_data(df, industry_key, series_key, region_key, time_key):
    df = df \
        .merge(
            industry_key.drop(columns='cat_indent'), on='cat_idx', how='left'
        ) \
        .merge(series_key[['dt_idx', 'dt_code']], on='dt_idx', how='left') \
        .merge(region_key, on='geo_idx', how='left') \
        .merge(time_key, on='per_idx', how='left') \
        .rename(
            columns={
                'cat_code':'naics', 'cat_desc':'industry', 'dt_code':'series', 
                'geo_code':'region_code', 'geo_desc':'region', 'per_name':'time'
            }
        ) \
        .assign(
            naics=lambda x: x.naics.str.replace('NAICS', '') \
                .replace({
                    'TOTAL':'00', 'TW':'48-49', 'RET':'44-45', 'MNF':'31-33', 
                    'NO':'ZZ'
                }),
        ) \
        .pivot(
            index=[
                'time', 'region', 'region_code', 'industry', 'naics', 'is_adj'
            ],
            columns='series', values='val'
        ) \
        .reset_index() \
        .replace({'D':np.NaN, 'S':np.NaN}) \
        .apply(pd.to_numeric, errors='ignore')
        
    df.columns.name = None

    return df


def _seasonal_adjust(df, seasonally_adj, series_lst, bf_helper_lst):
    if seasonally_adj and any([i.startswith('BF_DUR') for i in series_lst]):
        # Seasonal adjustment not available for DUR variables, so we just sub
        # in non-adjusted for DUR var and leave everything else the same
        index_var = [
            'time', 'region', 'region_code', 'industry', 'naics', 'is_adj'
        ]
        df_DUR = df[index_var + [s for s in series_lst if 'DUR' in s]] \
            .query('is_adj == False')
        df_non_DUR = df \
            [
                index_var \
                + [s for s in series_lst if 'DUR' not in s] \
                + bf_helper_lst
            ] \
            .query('is_adj == True')
        return df_DUR.merge(
            df_non_DUR,
            on=['time', 'region', 'region_code', 'industry', 'naics']
        )
    else:
        return df.query(f'is_adj == {seasonally_adj}')


def _query_data(
    df, region_lst, series_lst, bf_helper_lst, industry_lst, seasonally_adj
):
    return df \
        .pipe(_seasonal_adjust, seasonally_adj, series_lst, bf_helper_lst) \
        .query(f"region_code in {region_lst}") \
        .query(f"naics in {industry_lst}") \
        [
            ['time', 'region', 'region_code', 'industry', 'naics'] \
            + series_lst + bf_helper_lst
        ]


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
        df = df \
            .assign(BF_DUR4Q=lambda x: x['DUR4_numerator'] / x['BF_BF4Q']) \
            .drop('DUR4_numerator', 1)
    if 'BF_DUR8Q' in df.columns:
        df = df \
            .assign(BF_DUR8Q=lambda x: x['DUR8_numerator'] / x['BF_BF8Q']) \
            .drop('DUR8_numerator', 1)
    return df


def _annualize(df, annualize, bf_helper_lst, march_shift):
    index_cols = ['fips', 'region', 'region_code', 'time', 'industry', 'naics']
    if annualize:
        return df \
            .pipe(_time_annualize, march_shift) \
            .pipe(_DUR_numerator) \
            .groupby(index_cols) \
            .sum(min_count=12) \
            .pipe(_BF_DURQ) \
            .reset_index(drop=False) \
            .astype({'time':'int'}) \
            [[col for col in df.columns if col not in bf_helper_lst]]
    return df


def _bfs_data_create(
    region_lst, series_lst, industry_lst, seasonally_adj, annualize, march_shift
):
    if march_shift: annualize = True

    bf_helper_lst = []
    if annualize:
        if ('BF_DUR4Q' in series_lst) and ('BF_BF4Q' not in series_lst): 
            bf_helper_lst.append('BF_BF4Q')
        if ('BF_DUR8Q' in series_lst) and ('BF_BF8Q' not in series_lst): 
            bf_helper_lst.append('BF_BF8Q')

    df, industry_key, series_key, region_key, time_key = _fetch_data()

    return df \
        .pipe(clean_data, industry_key, series_key, region_key, time_key) \
        .pipe(
            _query_data, region_lst, series_lst, bf_helper_lst, industry_lst, 
            seasonally_adj
        ) \
        .assign(
            time=lambda x: pd.to_datetime(x['time'], format='%b%Y'),
            fips=lambda x: x.region_code.map(c.state_abb_to_fips)
        ) \
        .pipe(_annualize, annualize, bf_helper_lst, march_shift) \
        [['fips', 'region', 'naics', 'industry', 'time'] + series_lst] \
        .reset_index(drop=True)


def bfs(
    series_lst, obs_level='all', industry='00', seasonally_adj=True, 
    annualize=False, march_shift=False
):
    """ 
    Create a pandas data frame with results from a BFS query. 
    Column order: fips, region, time, series_lst.

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
            BF_DUR4Q: Average Duration (in Quarters) from Business Application
                to Formation within Four Quarters
            BF_DUR8Q: Average Duration (in Quarters) from Business Application 
                to Formation within Eight Quarters

    obs_level-- The level to pull observations for. ('state', 'us', or 'all')

    industry
        Variables:
            all: all industries and total
            '00': 'TOTAL',
            # todo: fix the values
            '11': 'NAICS11',
            '21': 'NAICS21',
            '22': 'NAICS22',
            '23': 'NAICS23',
            '31-33': 'NAICSMNF',
            '42': 'NAICS42',
            '44-45': 'NAICSRET',
            '48-49': 'NAICSTW',
            '51': 'NAICS51',
            '52': 'NAICS52',
            '53': 'NAICS53',
            '54': 'NAICS54',
            '55': 'NAICS55',
            '56': 'NAICS56',
            '61': 'NAICS61',
            '62': 'NAICS62',
            '71': 'NAICS71',
            '72': 'NAICS72',
            '81': 'NAICS81',
            'ZZ': 'NONAICS'

    seasonally_adj-- Option to use the census adjustment for seasonality and 
        smooth the time series. (True or False)

    annualize--Aggregates across months and annulizes data. (True or False)

    march_shift--When True: year end is March; False: year end is December. 
        (True or False)
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
            industry_lst = list(c.naics_code_to_abb(2).keys())
        else:
            industry_lst = [industry]

    return _bfs_data_create(
        region_lst, series_lst, industry_lst, seasonally_adj, annualize, 
        march_shift
    )
