import sys
import pandas as pd
import kauffman.constants as c
from kauffman.data import qwi
from kauffman.tools import mpj_indicators, county_msa_cross_walk, read_zip, state_msa_cross_walk

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 40000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def county_msa_cw():
    df = qwi(['Emp'], obs_level='county', state_list=['MO'], annualize=False).\
        pipe(county_msa_cross_walk, 'fips')
    print(df.head(10))


def state_msa_cw():
    df = state_msa_cross_walk(['29'])
    print(df)


def zip():
    url = 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip'
    df = read_zip(url, 'public2018.csv')
    print(df.head())


def mpj_industry():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    pd.concat(
            [
                mpj_indicators(
                    pd.read_csv(c.filenamer(f'../tests/data/qwi_{region}.csv')),
                    pd.read_csv(c.filenamer(f'../tests/data/pep_{region}.csv')),
                    df_earnbeg_us
                )
                for region in ['us', 'state']
                # for region in ['us', 'state', 'msa', 'county']
            ]
        ).\
        to_csv(c.filenamer(f'../tests/data/mpj_industry.csv'), index=False)


def mpj_covar():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    for covar in ['sex', 'agegrp', 'education', 'race_ethnicity']:
        df_temp = mpj_indicators(
            pd.read_csv(c.filenamer(f'../tests/data/qwi_us_{covar}.csv')).assign(fips=0),
            pd.read_csv(c.filenamer(f'../tests/data/pep_us.csv')),
            df_earnbeg_us
        )
        df_temp[covar] = df_temp[covar].map(c.mpj_covar_mapping(covar))
        df_temp['firmsize'] = df_temp['firmsize'].map(c.mpj_covar_mapping('firmsize'))
        df_temp.to_csv(c.filenamer(f'/Users/thowe/downloads/mpj_{covar}.csv'), index=False)
# todo: check these are correct
#     do these even make sense? like sex.
#     are the values correct?

if __name__ == '__main__':
    # mpj_industry()
    mpj_covar()

    # county_msa_cw()
    # state_msa_cw()
