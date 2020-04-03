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

def _renamer(df):
    return df.rename(columns=dict(zip(df.columns, map(lambda x: x.lower(), df.columns))))

def get_data(series_lst, obs_level, start_year, end_year=None, seasonally_adj=True, annualize=True):
    """
    series_lst: lst; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf.
        net_job_creation
        fage4
        fsize
        age4 is not a valid variable
        Can't have both fage4 and fsize if end_year > 2014.

    obs_level: lst
        us:
        state:

    start_year:
        earliest year with fill compliment of age categories is 2003
    """

    if (not end_year) or (end_year > 2016):
        end_year = 2016
    elif end_year < start_year:
        end_year == start_year
    print('Using {startyear} and {endyear} as start and end years.'.format(startyear=start_year, endyear=end_year))

    print('Creating dataframe', end='...')
    df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        print(year, end='...')
        if year <= 2014:
            url = 'https://api.census.gov/data/timeseries/bds/firms?get={series_lst}&for={obs_level}:*&time={year}'.format(series_lst=','.join(series_lst), obs_level=obs_level, year=year)
            r = requests.get(url)
            df_year = pd.DataFrame(r.json()).pipe(_make_header)
        else:
            df_year_age = pd.read_csv('http://www2.census.gov/ces/bds/{}/firm/bds_f_age_release.csv'.format(year)). \
                assign(fage4=lambda x: x['fage4'].str[0], us='00'). \
                pipe(_renamer). \
                rename(columns={'year2': 'time'}) \
                [series_lst + ['us', 'time']]
            df_year_all = pd.read_csv('http://www2.census.gov/ces/bds/{}/firm/bds_f_all_release.csv'.format(year)). \
                assign(fage4='m', us='00'). \
                pipe(_renamer). \
                rename(columns={'year2': 'time'}) \
                [series_lst + ['us', 'time']]
            df_year = df_year_age.append(df_year_all)

        df = df.append(df_year)
    # todo: convert the outcome variables to int or float
    # todo: time should be a string? might need to sort
    # todo: 2015 and 2016 if doing state
    return df.reset_index(drop=True)







if __name__ == '__main__':
    # df = get_data(['firms', 'net_job_creation', 'estabs', 'fage4'], 'us', 1977, end_year=2016).\
    #     astype({'firms': 'int', 'net_job_creation': 'int', 'estabs': 'int', 'time': 'int'})
    # print('\n')
    # print(df)

    # df = get_data(['estabs', 'firms', 'net_job_creation', 'fage4', 'fsize'], 'us', 2014, end_year=2015).\
    #     astype({'estabs': 'int', 'firms': 'int'})
    # print('\n')

    # df.pub.plot(['firms', 'estabs'], {'fage4': 'm', 'fsize': 'm'})
    # df.pub.plot({'firms': 'Firms', 'net_job_creation': 'Net Job Creation'}, {'fage4': 'm'}, save_path='/Users/thowe/Downloads/bds_net_job_creation.png')

    import kauffman_data.constants as c
    df = pd.read_csv(c.filenamer('../scratch/all_time.csv'))

    df.pub.plot(
        {'firms': 'Firms', 'net_job_creation': 'Net Job Creation'},
        strata_dic={'fage4': {'Startups': ['a'], 'Non-startups': ['g', 'h', 'i', 'j', 'k', 'l'], 'Young': ['a', 'b', 'c', 'd', 'e', 'f']}},
        # strata_dic={'fage4': {'Startups': ['a'], 'Non-startups': ['b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'], 'Young': ['a', 'b', 'c', 'd', 'e', 'f']}},
        to_index=False,
        years=list(range(1982, 2017)),
        recessions=True
    )
