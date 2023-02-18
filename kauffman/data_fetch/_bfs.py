import pandas as pd
import numpy as np
from kauffman import constants as c
from kauffman.tools import general_tools as g

ALL_VARIABLES = [
    'BA_BA', 'BA_CBA', 'BA_HBA', 'BA_WBA', 'BF_BF4Q', 'BF_BF8Q', 'BF_PBF4Q', 
    'BF_PBF8Q', 'BF_SBF4Q', 'BF_SBF8Q', 'BF_DUR4Q', 'BF_DUR8Q'
]

def _reshape_bfs(df):
    return df.melt(
            id_vars = ['year', 'geo', 'naics_sector', 'sa', 'series'],
            value_vars = [
                'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 
                'oct', 'nov', 'dec'
            ],
            var_name = 'month'
        ) \
        .pivot(
            index=['year', 'month', 'geo', 'naics_sector', 'sa'], 
            columns='series',
            values='value'
        ) \
        .reset_index()


def _clean_bfs_values(df, series_list):
    df = df \
        .assign(
            month=lambda x: x.month.str.capitalize(),
            is_adj=lambda x: x.sa.map({'A':True, 'U':False}),
            naics_sector=lambda x: x.naics_sector.map({
                **{
                    'TOTAL':'00', 'NONAICS':'ZZ', 'NAICSMNF':'31-33', 
                    'NAICSTW':'48-49', 'NAICSRET':'44-45'
                },
                **{f'NAICS{x}':x for x in [
                        '56', '81', '72', '71', '62', '61', '55', '11', '54', 
                        '53', '52', '51', '42', '23', '22', '21'
                    ]
                }
            })
        ) \
        .replace({'D': np.NaN, 'S': np.NaN})

    df[series_list] = df[series_list].apply(pd.to_numeric, errors='ignore')

    return df


def _seasonal_adjust(df, seasonally_adj, index, series_list, bf_helper_list):
    if seasonally_adj and any([i.startswith('BF_DUR') for i in series_list]):
        # Seasonal adjustment not available for DUR variables, so we just sub
        # in non-adjusted for DUR var and leave everything else the same
        df_DUR = df[index + ['is_adj'] + [s for s in series_list if 'DUR' in s]] \
            .query('is_adj == False')
        df_non_DUR = df \
            [
                index + ['is_adj']
                + [s for s in series_list if 'DUR' not in s]
                + bf_helper_list
            ] \
            .query('is_adj == True')
        return df_DUR.merge(
            df_non_DUR,
            on=index
        )
    else:
        return df.query(f'is_adj == {seasonally_adj}')


def _annualize(df, annualize, index, bf_helper_list, march_shift):
    if not annualize:
        return df

    # Annualize time variable
    df['time'] = df.time.apply(
        lambda x: x.year + 1 if march_shift and x.month > 3 else x.year
    )
    
    # Annualize main data
    for x in [4, 8]:
        if f'BF_DUR{x}Q' in df.columns:
            df[f'DUR{x}_numerator'] = df[f'BF_DUR{x}Q'] * df[f'BF_BF{x}Q']

    df = df.groupby(index).sum(min_count=12).reset_index()

    for x in [4, 8]:
        if f'BF_DUR{x}Q' in df.columns:
            df[f'BF_DUR{x}Q'] = df[f'DUR{x}_numerator'] / df[f'BF_BF{x}Q']
            df = df.drop(columns=f'DUR{x}_numerator')

    return df \
        .astype({'time':'int'}) \
        [[col for col in df.columns if col not in bf_helper_list]]


def bfs(
    series_list='all', geo_level='us', state_list='all', industry='00', 
    seasonally_adj=True, annualize=False, march_shift=False
):
    """
    Fetch and clean Business Formation Statistics (BFS) data from the following
    source: https://www.census.gov/econ/bfs/csv/bfs_monthly.csv.

    Parameters
    ----------
    series_list: list or 'all', optional
        List of variables to fetch. If 'all', the following variables will be 
        included:
        * BA_BA: 'Business Applications'
        * BA_CBA: 'Business Applications from Corporations'
        * BA_HBA: 'High-Propensity Business Applications'
        * BA_WBA: 'Business Applications with Planned Wages'
        * BF_BF4Q: 'Business Formations within Four Quarters'
        * BF_BF8Q: 'Business Formations within Eight Quarters'
        * BF_PBF4Q: Projected Business Formations within Four Quarters
        * BF_PBF8Q: Projected Business Formations within Eight Quarters
        * BF_SBF4Q: Spliced Business Formations within Four Quarter
        * BF_SBF8Q: Spliced Business Formations within Eight Quarters
        * BF_DUR4Q: Average Duration (in Quarters) from Business Application to
            Formation within Four Quarters
        * BF_DUR8Q: Average Duration (in Quarters) from Business Application to
            Formation within Eight Quarters
    geo_level: {'us', 'state'}, default 'us'
        The geographical level of the data.
    state_list: list or 'all', default 'all'
        The list of states to include in the data, identified by postal code 
        abbreviation. (Ex: 'AK', 'UT', etc.) Not available for geo_level = 'us'.
    industry: list or str or 'all', default '00'
        The industry (or industries) to include in the data. Only available for
        geo_level == 'us'. If 'all', the following variables will be included:
        * '00': 'TOTAL',
        * '11': 'NAICS11',
        * '21': 'NAICS21',
        * '22': 'NAICS22',
        * '23': 'NAICS23',
        * '31-33': 'NAICSMNF',
        * '42': 'NAICS42',
        * '44-45': 'NAICSRET',
        * '48-49': 'NAICSTW',
        * '51': 'NAICS51',
        * '52': 'NAICS52',
        * '53': 'NAICS53',
        * '54': 'NAICS54',
        * '55': 'NAICS55',
        * '56': 'NAICS56',
        * '61': 'NAICS61',
        * '62': 'NAICS62',
        * '71': 'NAICS71',
        * '72': 'NAICS72',
        * '81': 'NAICS81',
        * 'ZZ': 'NONAICS'
    seasonally_adj: bool, default True
        Whether to fetch the to use the census adjustment for seasonality and 
        smooth the time series.
    annualize: bool, default False
        Whether to aggregate the data to the annual level.
    march_shift: bool, default False
        Whether to use a "march shift" annualization method, wherein Q2 is 
        considered the start of the year.
    """
    series_list = ALL_VARIABLES if series_list == 'all' else series_list
    
    state_list = c.STATES if state_list == 'all' else state_list    
    region_list = state_list if geo_level == 'state' else ['US']

    if type(industry) == list:
        industry_list = industry
    elif industry == 'all':
        industry_list = list(g.naics_code_key(2)['naics'])
    else:
        industry_list = [industry]

    if march_shift: annualize = True

    bf_helper_list = []
    if annualize:
        if ('BF_DUR4Q' in series_list) and ('BF_BF4Q' not in series_list): 
            bf_helper_list.append('BF_BF4Q')
        if ('BF_DUR8Q' in series_list) and ('BF_BF8Q' not in series_list): 
            bf_helper_list.append('BF_BF8Q')

    index = ['time', 'fips', 'region', 'naics', 'industry']

    return pd.read_csv('https://www.census.gov/econ/bfs/csv/bfs_monthly.csv') \
        .pipe(_reshape_bfs) \
        .pipe(_clean_bfs_values, series_list + bf_helper_list) \
        .rename(columns={'naics_sector':'naics', 'geo':'region_code'}) \
        .assign(
            time=lambda x: pd.to_datetime(
                x['year'].astype(str) + x['month'], 
                format='%Y%b'
            ),
            fips=lambda x: x.region_code.map(c.STATE_ABB_TO_FIPS),
            region=lambda x: x.region_code.map(c.STATE_ABB_TO_NAME)
        ) \
        .merge(g.naics_code_key(2)).rename(columns={'name': 'industry'}) \
        .query(f"region_code in {region_list} and naics in {industry_list}") \
        .pipe(
            _seasonal_adjust, seasonally_adj, index, series_list, bf_helper_list
        ) \
        [index + series_list + bf_helper_list] \
        .pipe(_annualize, annualize, index, bf_helper_list, march_shift) \
        .dropna(subset=series_list, how='all') \
        .sort_values(index) \
        .reset_index(drop=True)