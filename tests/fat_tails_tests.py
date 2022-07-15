import sys
import pandas as pd
import seaborn as sns
from kauffman.data import bfs
import kauffman.constants as c
import matplotlib.pyplot as plt
from kauffman.tools import alpha, log_log_plot, maximum_to_sum_plot, excess_conditional_expectation, \
    maximum_quartic_variation, M, kappa, n_v


def log_log():
    # df = bfs(['BA_BA'], obs_level='us'). \
    #     query('BA_BA == BA_BA'). \
    #     sort_values('BA_BA', ascending=False). \
    #     reset_index(drop=True). \
    #     assign(p=lambda x: (x.index + 1) / x.shape[0])
    # log_log_plot(df, 'BA_BA', 'Business Applications', threshold = 5.375)
    # print(alpha(df, 'BA_BA', threshold=5.375))

    df = pd.read_csv(c.filenamer(f'../tests/data/RawData.csv')). \
        assign(time=lambda x: pd.to_datetime(x['TransDateTime'])). \
        sort_values('RespTime', ascending=False). \
        reset_index(drop=True). \
        assign(p=lambda x: (x.index + 1) / x.shape[0])
    log_log_plot(df, 'RespTime', 'Response Time', threshold=4.4)
    print(alpha(df, 'RespTime', threshold=4.4))


def maximum_to_sum():
    # bfs(['BA_BA'], obs_level='us'). \
    #     query('BA_BA == BA_BA'). \
    #     reset_index(drop=True). \
    #     pipe(maximum_to_sum_plot, 'BA_BA', 'Business Applications')

    pd.read_csv(c.filenamer(f'../tests/data/RawData.csv')).\
        assign(time=lambda x: pd.to_datetime(x['TransDateTime'])).\
        sort_values('time', ascending=True). \
        sample(50_000). \
        assign(RespTime=lambda x: x['RespTime'] / 1000). \
        pipe(maximum_to_sum_plot, 'RespTime', 'Response Time')


def k1_and_n_v_estimate():
    df = pd.read_csv(c.filenamer(f'../tests/data/RawData.csv')).\
        assign(time=lambda x: pd.to_datetime(x['TransDateTime'])).\
        sort_values('time', ascending=True)

    # find kappa_1
    k = kappa(df, n=2, n0=1)
    print(k)

    # find n_v
    n = n_v(k, 30)
    print(n)


def kappa_convergence_plot():
    df = pd.read_csv(c.filenamer(f'../tests/data/RawData.csv')).\
        assign(time=lambda x: pd.to_datetime(x['TransDateTime'])).\
        sort_values('time', ascending=True)
    n_lst = range(2, 1003, 50)

    kappa_lst = [kappa(df, n=n, n0=1) for n in n_lst]

    sns.set_style("whitegrid")
    fig = plt.figure(figsize=(20, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.scatter(n_lst, kappa_lst)
    ax.set_ylim([0, .9])
    plt.show()


def other():
    df = bfs(['BA_BA'], obs_level='us'). \
        query('BA_BA == BA_BA'). \
        reset_index(drop=True)

    excess_conditional_expectation(df, 'BA_BA', 'Business Applications', sign='positive')
    maximum_quartic_variation(df, 'BA_BA')


if __name__ == '__main__':
    maximum_to_sum()
    # log_log()
    # k1_and_n_v_estimate()
    # kappa_convergence_plot()
    # other()
