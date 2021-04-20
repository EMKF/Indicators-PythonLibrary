from context import kauffman

from kauffman.data import acs, bfs, bds, pep, bed, qwi
from kauffman.tools import alpha, log_log_plot, maximum_to_sum_plot, excess_conditional_expectation, \
    maximum_quartic_variation

def _data_fetch():
    # todo: test this against what I'm seeing in kauffman_indicators

    # df = acs(['B24092_004E', 'B24092_013E'])

    # df = qwi(indicator_lst=['Emp', 'HirA'], obs_level='us', annualize=None, strata=['sex', 'industry'])
    # df = qwi(obs_level='state')


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
    df = pep(obs_level='state')

    print(df)
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


def _cross_walk():
    pass

if __name__ == '__main__':
    _data_fetch()
    # _distribution_tests()
    # _cross_walk()

#     df = get_data(['B24081_001E', 'B24092_017E', 'B24092_018E'])
#     print(df)
#     sys.exit()
#     # raw_data_createB24081()
#     # raw_data_createB24092()
#
#     # harry_plotter_overall()
#     # harry_plotter_gender()
#
#     agg_raw_data_create()
#
