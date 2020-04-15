import sys
import joblib
import requests
import numpy as np
import pandas as pd
import kauffman_data.constants as c
import kauffman_data.public_data_helpers as p

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def scratch_panel_to_alley():
    df = pd.read_csv(c.filenamer('../scratch/jobs_indicators_sample_all.csv')).\
        rename(columns={'Age': 'Age of Business'})
    df.loc[df['Age of Business'] == '0-1', 'Age of Business'] = 'Ages 0 to 1'
    df.loc[df['Age of Business'] == '2-3', 'Age of Business'] = 'Ages 2 to 3'
    df.loc[df['Age of Business'] == '4-5', 'Age of Business'] = 'Ages 4 to 5'
    df.loc[df['Age of Business'] == '6-10', 'Age of Business'] = 'Ages 6 to 10'
    df.loc[df['Age of Business'] == '11+', 'Age of Business'] = 'Ages 11+'

    df_overall = df.loc[df['Age of Business'] == 'Ages 0 to 1']
    df_overall.loc[:, 'Age of Business'] = 'overall'

    df_in = df.\
        append(df_overall).\
        sort_values(['fips', 'year', 'Age of Business'])

    df_out = df_in.pub.panel_to_alley(['Age of Business'], 'Age Share of Employment')
    print(df_out.head(65))


def scratch_kese_to_panel():
    p.fairlie_to_panel(c.filenamer('../scratch/Kauffman Indicators Data State 1996_2019_v3.xlsx'), c.filenamer('../scratch/Kauffman Indicators Data National 1996_2019_v3.xlsx'))

def main():
    # scratch_panel_to_alley()
    scratch_kese_to_panel()

if __name__ == '__main__':
    main()