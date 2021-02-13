import sys
import joblib
import requests
import pandas as pd
from scratch import constants as c


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def _fetch_data(year, var_set):
    var_lst = ','.join(var_set)
    url = 'https://api.census.gov/data/{year}/acs/acs1?get={var_lst}&for=us:*'.format(var_lst=var_lst, year=year)
    r = requests.get(url)
    return pd.DataFrame(r.json())


def _make_header(df):
    df.columns = df.iloc[0]
    return df.iloc[1:, :]


def raw_data_createB24081(save=True):
    df = pd.DataFrame()
    for year in range(2005, 2019):
        df = df.append(
            _fetch_data(year, 'B24081').\
            pipe(_make_header).\
            rename(columns=
                {
                    'B24081_001E': 'total',
                    'B24081_002E': 'private',
                    'B24081_003E': 'private_employee',
                    'B24081_004E': 'private_self_employed',
                    'B24081_005E': 'non_profit',
                    'B24081_006E': 'local_government',
                    'B24081_007E': 'state_government',
                    'B24081_008E': 'federal_government',
                    'B24081_009E': 'self_employed_not_inc'
                }
            ).\
            drop('us', 1).\
            transpose().\
            reset_index().\
            rename(columns={0: 'employment_type', 1: 'median_earnings'}).\
            assign(year=year)
        )
    if save:
        joblib.dump(df.reset_index(drop=True), c.filenamer('acs/data/median_earnings_2005_2018_B24081.pkl'))
    else:
        return df


def raw_data_createB24092(save=True):
    df = pd.DataFrame()
    for year in range(2005, 2019):
        df = df.append(
            _fetch_data(year, 'B24092').\
            pipe(_make_header).\
            rename(columns=
                {
                    'B24092_001E': 'total_m',
                    'B24092_002E': 'private_m',
                    'B24092_003E': 'private_employee_m',
                    'B24092_004E': 'private_self_employed_m',
                    'B24092_005E': 'non_profit_m',
                    'B24092_006E': 'local_government_m',
                    'B24092_007E': 'state_government_m',
                    'B24092_008E': 'federal_government_m',
                    'B24092_009E': 'self_employed_not_inc_m',
                    'B24092_010E': 'total_f',
                    'B24092_011E': 'private_f',
                    'B24092_012E': 'private_employee_f',
                    'B24092_013E': 'private_self_employed_f',
                    'B24092_014E': 'non_profit_f',
                    'B24092_015E': 'local_government_f',
                    'B24092_016E': 'state_government_f',
                    'B24092_017E': 'federal_government_f',
                    'B24092_018E': 'self_employed_not_inc_f'
                }
            ).\
            drop('us', 1).\
            transpose().\
            reset_index().\
            rename(columns={0: 'employment_type', 1: 'median_earnings'}).\
            assign(year=year)
        )
    if save:
        joblib.dump(df.reset_index(drop=True), c.filenamer('acs/data/median_earnings_2005_2018_B24092.pkl'))
    else:
        return df


def agg_raw_data_create():
    df_overall = joblib.load(c.filenamer('acs/data/median_earnings_2005_2018_B24081.pkl')).\
        assign(gender='overall')

    df_gender = joblib.load(c.filenamer('acs/data/median_earnings_2005_2018_B24092.pkl'))
    df_gender['gender'] = df_gender.apply(lambda x: 'male' if x['employment_type'][-1] == 'm' else 'female', axis=1)
    df_gender['employment_type'] = df_gender['employment_type'].apply(lambda x: x[:-2])

    df = df_overall.append(df_gender).reset_index(drop=True)
    joblib.dump(df, c.filenamer('acs/data/median_earnings_2005_2018_gender.pkl'))
    df.to_csv(c.filenamer('acs/data/median_earnings_2005_2018_gender.csv'), index=False)


# todo: relabel here...this isn't great.
var_dict = {
    'B24081_001E': 'total',
    'B24081_002E': 'private',
    'B24081_003E': 'private_employee',
    'B24081_004E': 'private_self_employed',
    'B24081_005E': 'non_profit',
    'B24081_006E': 'local_government',
    'B24081_007E': 'state_government',
    'B24081_008E': 'federal_government',
    'B24081_009E': 'self_employed_not_inc',
    'B24092_001E': 'total_m',
    'B24092_002E': 'private_m',
    'B24092_003E': 'private_employee_m',
    'B24092_004E': 'private_self_employed_m',
    'B24092_005E': 'non_profit_m',
    'B24092_006E': 'local_government_m',
    'B24092_007E': 'state_government_m',
    'B24092_008E': 'federal_government_m',
    'B24092_009E': 'self_employed_not_inc_m',
    'B24092_010E': 'total_f',
    'B24092_011E': 'private_f',
    'B24092_012E': 'private_employee_f',
    'B24092_013E': 'private_self_employed_f',
    'B24092_014E': 'non_profit_f',
    'B24092_015E': 'local_government_f',
    'B24092_016E': 'state_government_f',
    'B24092_017E': 'federal_government_f',
    'B24092_018E': 'self_employed_not_inc_f'
}


def get_data(series_lst, start_year=2005, end_year=2018):
    """
    https://api.census.gov/data/2019/acs/acs1/variables.html
    """
    df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        df = df.append(
            _fetch_data(year, series_lst).\
            pipe(_make_header).\
            rename(columns=var_dict).\
            drop('us', 1).\
            reset_index(drop=True).\
            assign(year=year)
        )
    return df



def main():
    df = get_data(['B24081_001E', 'B24092_017E', 'B24092_018E'])
    print(df)
    sys.exit()
    # raw_data_createB24081()
    # raw_data_createB24092()

    # harry_plotter_overall()
    # harry_plotter_gender()

    agg_raw_data_create()


if __name__ == '__main__':
    main()
