import sys
import numpy as np
import pandas as pd


def kese_indicators():
    pass


def _neb_raw_data_merge(df_bfs, df_pep, df_bds, df_bfs_march):
    return df_bfs. \
        merge(df_pep.drop('region', 1), how='left', on=['fips', 'time']).\
        merge(df_bds.drop('region', 1), how='left', on=['fips', 'time']).\
        merge(df_bfs_march.drop('region', 1), how='left', on=['fips', 'time'])

def neb_indicators(df_bfs, df_pep, df_bds, df_bfs_march):
    # todo: velocity?
    return _neb_raw_data_merge(df_bfs, df_pep, df_bds, df_bfs_march). \
        rename(columns={'avg_speed_annual': 'velocity'}). \
        assign(
            actualization=lambda x: x['bf'] / x['ba'],
            bf_per_capita=lambda x: x['bf'] / x['population'] * 100,
            newness=lambda x: x['bf_march_shift'] / x['firms'],
        )


def _mpj_raw_data_merge(df_qwi, df_pep, df_earnbeg_us):
    return df_qwi. \
        merge(df_pep[['fips', 'time', 'population']], how='left', on=['fips', 'time']).\
        merge(df_earnbeg_us, how='left', on='time')


def _missing_obs(df):
    df.loc[df['EmpTotal'] == 0, 'constancy'] = np.nan
    df.loc[df['EarnBeg'] == 0, 'compensation'] = np.nan
    df.loc[df['emp_mid'] == 0, 'contribution'] = np.nan
    return df

def _total_emp_create(df, contribution_by):
    covars = ['fips', 'time']
    if contribution_by:
        covars = ['fips', 'time'] + contribution_by.split('_')

    return df.\
        assign(
            within_count=lambda x: x[['emp_mid'] + covars].groupby(covars).transform('count'),
            max_count=lambda x: x['within_count'].max(),
            total_emp=lambda x: x[['emp_mid'] + covars].groupby(covars).transform(sum, min_count=int(x['max_count'].iloc[0]))
        )

def mpj_indicators(df_qwi, df_pep, df_earnbeg_us, contribution_by=None, constancy_mult=1):
    """
    todo: description of each of these data sets briefly


    contribution_by: str, None
        None: unconditional employment count, across all strata
        covariate name: covariate name to condition on
    """
    # todo: need some restrictions for variable names in each of the dfs
    #   each row needs to be unique category within fips/time
    # todo: I want to drop the variables from pep and earnbeg_us

    return _mpj_raw_data_merge(
            df_qwi,
            df_pep,
            df_earnbeg_us.rename(columns={'EarnBeg': 'EarnBeg_us'})
        ).\
        assign(
            emp_mid=lambda x: (x['Emp'] + x['EmpEnd']) / 2,
        ).\
        pipe(_total_emp_create, contribution_by).\
        assign(
            contribution=lambda x: x['emp_mid'] / x['total_emp'],
            compensation=lambda x: x['EarnBeg'] / x['EarnBeg_us'],
            constancy=lambda x: (x['EmpS'] / x['EmpTotal']) * constancy_mult,
            creation=lambda x: x['FrmJbC'] / x['population'] * 1000,
        ).\
        pipe(_missing_obs). \
        drop(['ownercode', 'emp_mid', 'within_count', 'max_count', 'total_emp', 'Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC', 'population', 'EarnBeg_us'], 1)
