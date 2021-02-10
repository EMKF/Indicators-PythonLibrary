import pandas as pd
import kauffman.constants as c
from kauffman.bfs_helpers import _region_data_create

def bfs(series_lst, obs_level='state', seasonally_adj=True, annualize=False):
    """
    series_lst: lst

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


    """

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level == 'us':
            region_lst = ['US']
        else:
            region_lst = c.states
    return pd.concat(
            [
                _region_data_create(region, series_lst, seasonally_adj, annualize)
                for region in region_lst
            ],
            axis=0
        ).\
        reset_index(drop=True)


def bds(series_lst, obs_level='state', seasonally_adj=True, annualize=False):
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
    return df.\
        reset_index(drop=True).\
        astype({'time': 'str'})
