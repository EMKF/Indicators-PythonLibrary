#from context import kauffman

import sys
import pandas as pd
from kauffman.data import acs, bfs, bds, pep, bed, qwi
from kauffman.tools import alpha, log_log_plot, maximum_to_sum_plot, excess_conditional_expectation, \
    maximum_quartic_variation
from kauffman.tools.etl import county_msa_cross_walk as cw, read_zip


pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 40000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)



def _data_fetch():

    # df = acs(['B24092_004E', 'B24092_013E'])

    # df = qwi(obs_level='state', state_list=['MO'], annualize=False)
    # df = qwi(obs_level='state', state_list=['MO'])
    df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, strata=['firmage'], annualize=True)
    print(df.head(10))
    sys.exit()


    # todo: why are some outcomes vastly different when aggregating counties? Calculations?
    qwi(obs_level='msa', state_list=['CO']).\
        pipe(lambda x: print(x))

    qwi(obs_level='county', state_list=['CO']). \
        pipe(cw, 'fips'). \
        drop(['fips_county', 'region'], 1). \
        groupby(['fips_msa', 'CBSA Title', 'time']).sum(). \
        reset_index(drop=False). \
        rename(columns={'CBSA Title': 'region', 'fips_msa': 'fips'}).\
        query('fips == "19740"').\
        pipe(lambda x: print(x))
    sys.exit()


    # todo: test this against what I'm seeing in kauffman_indicators
    # df = qwi(indicator_lst=['Emp', 'HirA'], obs_level='us', annualize=None, strata=['sex', 'industry'])

    print(df)
    sys.exit()


    df = qwi(obs_level='county', state_list=['CO'])
    # df = qwi(obs_level='state', state_list=['UT'], strata=['firmsize'])
    # df = qwi(obs_level='msa', state_list=['CO', 'UT'], strata=['firmsize'])

    df = qwi(obs_level='state')
    df = qwi(obs_level='msa')
    df = qwi(obs_level='county')


    # df = bed('firm size', 1)
    # df = bed('firm size', 2)
    # df = bed('firm size', 3)
    # df = bed('firm size', 4)
    # df = bed('1bf', obs_level=['AL', 'US', 'MO'])

    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['AZ'])
    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)

    # df = bds(['FIRM', 'ESTAB'], obs_level='all')

    # df = pep(obs_level='us')
    # df = pep(obs_level='state')
    # df = pep(obs_level='msa')
    # df = pep(obs_level='county')

    print(df.head())
    print(df.tail())
    print(df.info())



def _distribution_tests():
    # df = bfs(['BA_BA'], obs_level='us'). \
    #     query('BA_BA == BA_BA'). \
    #     sort_values('BA_BA', ascending=False). \
    #     reset_index(drop=True). \
    #     assign(p=lambda x: (x.index + 1) / x.shape[0])
    # log_log_plot(df, 'BA_BA', 'Business Applications', threshold = 5.375)
    # print(alpha(df, 'BA_BA', threshold=5.375))

    df = bfs(['BA_BA'], obs_level='us'). \
        query('BA_BA == BA_BA'). \
        reset_index(drop=True)
    # maximum_to_sum_plot(df, 'BA_BA')
    # excess_conditional_expectation(df, 'BA_BA', 'Business Applications', sign='positive')
    maximum_quartic_variation(df, 'BA_BA')


def _etl_tests():
    # df = qwi(['Emp'], obs_level='county', annualize=False).\
    #     pipe(county_msa_cross_walk, 'fips')
    # print(df.head(10))

    url = 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip'
    df = read_zip(url, 'public2018.csv')
    print(df.head())



if __name__ == '__main__':
     _data_fetch()
    # _distribution_tests()
    # _etl_tests()
