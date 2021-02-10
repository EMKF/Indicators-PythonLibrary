import sys
import requests
import pandas as pd


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


if __name__ == '__main__':
    df = get_data(['firms', 'net_job_creation', 'estabs', 'fage4'], 'us', 1977, end_year=2016).\
        astype({'firms': 'int', 'net_job_creation': 'int', 'estabs': 'int', 'time': 'int'})
    print('\n')
    print(df.head())
