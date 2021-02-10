import io
import os
import json
import requests
import pandas as pd

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def get_data_api(series_lst='self_employed', obs_level='us', start_year=None, end_year=None, seasonally_adj=True):
    """'Year range has been reduced to the system-allowed limit of 20 years.' Cuss.
    """
    headers = {'Content-type': 'application/json'}
    data = {
            "seriesid": ['LNU02048984', 'LNU02027714'],
            "registrationkey": os.getenv('BLS_KEY'),
        }
    if start_year:
        data["startyear"] = "{start_year}".format(start_year=start_year)
        if end_year:
            data["endyear"] = "{end_year}".format(end_year=end_year)
        else:
            data["endyear"] = pd.datetime.now().year

    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=json.dumps(data), headers=headers)
    print(p.status_code)
    print(p.json())
    for ind, series in enumerate(p.json()['Results']['series']):
        df_temp = pd.DataFrame.from_dict(series['data']). \
            rename(columns={'value': series['seriesID'], 'latest': 'latest_{}'.format(series['seriesID'])}). \
            drop('footnotes', 1)
        print(df_temp.head())
        print(df_temp.info())

        if ind == 0:
            df = df_temp
        else:
            df = df.merge(df_temp, how='right', on=['year', 'period', 'periodName'])
    return df


def _annualizer(df, annualize):
    if annualize:
        df_out = df.\
            query('period == "M13"').\
            drop('period', 1)

        no_annual_avg_lst = [series for series in df.columns if series in ['unemployment_rate', 'civilian_labor_force']]
        if no_annual_avg_lst:
            for series in no_annual_avg_lst:
                df_out = df_out.\
                    drop(series, 1).\
                    merge(
                        df[['year', series]].groupby('year').mean().reset_index(),
                        how='outer',
                        on='year'
                    )
        return df_out.rename(columns={'year': 'time'})

    return df.\
        query('period != "M13"'). \
        assign(time=lambda x: x['year'].astype(str) + '-' + x['period'].str[1:]). \
        drop(['year', 'period'], 1)


def get_data(series_lst, obs_level='us', start_year=None, end_year=None, seasonally_adj=True, annualize=False):
    """
    The self-employment data for sub-US levels must come from IRS SOI data: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-2017-zip-code-data-soi.

    The period column value "M13" indicates an annual average.

    https://beta.bls.gov/dataQuery/find?st=0&r=20&q=self-employed&more=0&fq=survey:[ln]&s=id:A
    (The best way of figuring out the series number is clicking on "One Screen Data Search" in the Labor Force
    Statistics row from https://www.bls.gov/cps/data.htm; secondarily, trying to navigate
    https://download.bls.gov/pub/time.series/ln might be helpful, but less so).
    https://beta.bls.gov/dataViewer/view/timeseries/LNU04000000....switch up the series number


    plot unincorporated self-employment, incorporated self-employment, and wage-takers full-time,



    series_lst: int
        civilian_labor_force: BLS series number LNU11000000 --- Civilian labor force; seasonally adjusted; number
            reported in thousands of dollars but changed into dollars.
        unemployment_rate: BLS series number LNS14000000 --- Unemployment rate, 16 years and over
            TODO: When I'm ready to do this for more granular regions look at https://download.bls.gov/pub/time.series/la/ and https://download.bls.gov/pub/time.series/la/la.txt

        employed_not_ise: BLS series number LNU02574601 --- Employment level of private wage and salary workers,
            excluding the incorporated self-employed; not seasonally adjusted; number reported in thousands but changed
            to unit.
        inc_self_employment: BLS series number LNU02048984 --- Employment level of incorporated, self-employed workers;
            not seasonally adjusted; number reported in thousands of dollars but changed into dollars
        uninc_self_employment: BLS series number LNU02027714 --- Employment level of unincorporated,
            self-employed workers; not seasonally adjusted; number reported in thousands of dollars but changed into
            dollars.

    LNS12500000: Employed full time (persons who usually work 35 hours or more); seasonally adjusted
    LNS12000000
    LNU01000000: Civilian Labor Force level
    LNU02000000: unadjusted employment level
    LNU02300000: employment population ratio
    LNU02500000: employed, usually work full-time unadjusted
    LNU02600000: employed, usually work part-time unadjusted
    LNU03000000: unemployment level
    LNU03100000: unemployment level, looking for full-time work
    LNU03200000: unemployment level, looking for part-time work
    LNU04000000: unemployment rate
    LNU04100000: unemployment rate, full-time labor force
    LNU04200000: unemployment rate, part-time labor force
    LNU05000000: not in labor force
    LNU06000000:
    LNU07000000:
    LNU07100000:
    LNU07200000:
    LNU07300000:
    LNU07400000:
    LNU07500000:
    LNU07600000:
    LNU07700000:
    LNU07800000:
    LNU07900000:
    LNU08000000:
    LNU08100000:
    LNU08200000:
    LNU08300000:
    LNU08400000:

    annualize: bool
        If True, then returns the annual average of each series.
    """

    # if there is no dip in incorporated self-employment and there isn't a search in incorporating, then this tells us something about vulnerability, no?


    print('Creating dataframe (this may take a few minutes)...')
    r = requests.get('https://download.bls.gov/pub/time.series/ln/ln.data.1.AllData')
    df = pd.read_csv(io.BytesIO(r.content), sep='\t').\
        rename(columns=lambda x: x.strip()). \
        assign(series_id=lambda x: x['series_id'].str.strip()). \
        drop('footnote_codes', 1)

    # print(df['series_id'].unique().tolist())
    # print(df.loc[df['series_id'].str.contains('U')]['series_id'].unique().tolist())

    # df_out = df.query('series_id == "LNU02574601"')
    # print(df_out.info())
    # print(df_out.head())
    # sys.exit()
    # todo: can we get the full-time employed


    # data_dict = {
    #     series: df. \
    #         query('series_id == "{series_id}"'.format(series_id=series)). \
    #         drop('series_id', 1). \
    #         rename(columns={'value': series}).\
    #         astype({series: 'float'})
    #     for series in series_lst
    # }
    #
    # print(data_dict)
    # sys.exit()

    for ind, series in enumerate(series_lst):
        df_temp = df. \
            query('series_id == "{series_id}"'.format(series_id=series)). \
            drop('series_id', 1). \
            rename(columns={'value': series}).\
            astype({series: 'float'})
        if ind == 0:
            df_out = df_temp
        else:
            df_out = df_out.merge(df_temp, how='outer', on=['year', 'period'])

    # todo: if period begins with A then annual must be true
    # sys.exit()
    if not start_year:
        start_year = df_out['year'].min()
    if not end_year:
        end_year = df_out['year'].max()
    print('Data download complete.\nUsing {startyear} and {endyear} as start and end years.\nReformatting data.'.format(startyear=start_year, endyear=end_year))

    return df_out.\
            query('year >= {start_year}'.format(start_year=start_year)).\
            query('year <= {end_year}'.format(end_year=end_year)).\
            pipe(_annualizer, annualize).\
            sort_values('time').\
            reset_index(drop=True) \
            [['time'] + series_lst]


if __name__ == '__main__':
    import time
    start = time.time()
    # df = get_data(['inc_self_employment'], annualize=True)
    # df = get_data(['LNU02574601', 'LNU02048984', 'LNU02027714'], annualize=False)
    df = get_data(['LNU02500000', 'LNU02600000', 'LNU02048984', 'LNU02027714'], annualize=False)
    print(time.time() - start)
    print(df.info())
    print(df.head())
    print(df.tail())


# todo: farm/nonfarm?