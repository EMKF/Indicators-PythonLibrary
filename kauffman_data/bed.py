# todo
import sys
import joblib
import pandas as pd
from scratch.old import constants as c
import kauffman_data.public_data_helpers


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def index_cut(df, criterion, direction):
    ind = df.loc[df[0] == criterion].index.tolist()[0]
    if direction == 'above':
        return df.loc[:ind, :].reset_index(drop=True)
    return df.iloc[ind + 4:, [0, 2]]


def _fetch_data(state, series):
    url = 'https://www.bls.gov/bdm/age_by_size/{0}_age_naics_size_ein_20181_{1}.xlsx'.format(state, series)
    print('{0}: {1}'.format(state, url))
    return pd.read_excel(url, header=None).\
        pipe(index_cut, "Size class: All sizes", 'below').\
        pipe(index_cut, 2018, 'above').\
        rename(columns={0: 'year', 2: 'value'})


def raw_data_create():
    for series in ['t1', 't3']:
        df = pd.DataFrame()
        for state in c.states:
            df_temp = _fetch_data(state.lower(), series)
            df_temp['state'] = state
            df = df.append(df_temp)
        joblib.dump(df, c.filenamer('data/raw_states_{}.pkl'.format(series)))


def data_transform():
    df_t1 = joblib.load(c.filenamer('data/raw_states_t1.pkl')).rename(columns={'value': 'gross_job_gains'})
    df_t3 = joblib.load('data/raw_states_t3.pkl').rename(columns={'value': 'establishments'})

    df = df_t1.merge(df_t3, how='left', on=['year', 'state'])
    print(df.info()); sys.exit()
    joblib.dump(df, 'data/yearly_states_jobs.pkl')
    df.to_csv('data/yearly_states_jobs.csv')


def main():
    raw_data_create()
    data_transform()


if __name__ == '__main__':
    main()
