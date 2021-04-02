from context import kauffman

from kauffman.data import bfs, bds, pep, bed
from kauffman.tools import alpha, log_log_plot, maximum_to_sum_plot, excess_conditional_expectation

def data_fetch():
    df = bed('1bf', obs_level=['AL', 'US', 'MO'])
    # df = bed('1bf', obs_level=['AL', 'US', 'MO'])

    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['AZ'])
    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)

    # df = bds(['FIRM', 'ESTAB'], obs_level='all')

    # df = pep(obs_level='us')
    # df = pep(obs_level='state')

    print(df.info())
    print(df.head())
    print(df.tail())


def data_tools():
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
    excess_conditional_expectation(df, 'BA_BA', 'Business Applications', sign='positive')


if __name__ == '__main__':
    # data_fetch()
    data_tools()