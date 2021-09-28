# import kauffman
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression


def _alpha_model_fit(df):
    model = LinearRegression()
    model.fit(df[['log_outcome']], df['log_p'])
    return model
    # return model.coef_[0]

def _alpha_estimate(df, outcome, threshold):
    df_temp = df.\
        assign(
            log_outcome=lambda x: np.log10(x[outcome]),
            log_p=lambda x: np.log10(x['p'])
        )

    model = _alpha_model_fit(
        df_temp.\
            query(f'log_outcome >= {threshold}').\
            query('log_outcome == log_outcome')
    )

    return model.coef_[0], model.intercept_


def alpha(df, outcome, threshold):
    return -_alpha_estimate(df, outcome, threshold)[0]


def log_log_plot(df, outcome, outcome_label, threshold=None):
    """Makes a log-log plot, also known as a Zipf plot. A variable representing the survival probability needs to be in
    the dataframe and named 'p'.

    threshold: float or None
        If not None, then will calculate and plot the slope of the tail.
    """
    # todo: error if no column names p

    sns.set_style("whitegrid")  # , {'axes.grid': False})
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.scatter(df[outcome], df['p'])
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel(outcome_label)
    ax.set_ylabel('$P_{>|X|}$')

    if threshold:
        a, b = _alpha_estimate(df, outcome, threshold)
        ax.plot(df[outcome], (df[outcome] ** a) * (10 ** b), color='firebrick')

    plt.title(f'Log-Log Plot of {outcome_label}: $\\alpha == {round(-a, 2)}$')
    plt.show()


def maximum_to_sum_plot(df, outcome, outcome_label, moment=4):
    """Maximum-to-Sum plot. Test 4 in chapter 10, page 192"""
    n = df.shape[0]
    ms_lst = []
    for num in range(1, n + 1):
        df_temp = df[outcome].iloc[0:num] ** moment
        ms_lst.append(df_temp.max() / df_temp.sum())

    sns.set_style("whitegrid")
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(range(n), ms_lst)
    ax.set_xlabel('n')
    ax.set_ylabel(f'MS({moment})')

    plt.title(f'Maximum-to-Sum Plot for {outcome_label}')
    plt.show()


def excess_conditional_expectation(df, outcome, outcome_label, sign='positive'):
    '''
    Test 2 in chapter 10, page 190


    '''
    if sign == 'positive':
        sign = 1
    else:
        sign = -1

    k_lst = np.linspace(df[outcome].min(), df[outcome].max(), 1000)

    ce_lst = [(sign * df.query(f'{sign} * {outcome} > {k}')[outcome]).mean() / k for k in k_lst]

    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(k_lst, ce_lst)
    ax.set_xlabel('K')
    ax.set_ylabel('$\\frac{E(X) |_{X>K}}{K}$')

    plt.title(f'Excess Conditional Expectation for {outcome_label}')
    plt.show()


def maximum_quartic_variation(df, outcome):
    """
    The contribution of the maximum quartic variations over n samples.

    The frequency should be decided prior calling this function.
    """
    Q_n = (df[outcome] ** 4).max() / (df[outcome] ** 4).sum()
    print(f'Maximum quartic variations: {round(Q_n, 3)}')


def sigma_calc(df_ba):
    sigma = (df_ba['BA_BA'].max() - df_ba['BA_BA'].mean()) / df_ba['BA_BA'].std()
    print(f'Number of standard deviations: {sigma}')  # close to 6...It's a six sigma event!


def M(data, n, bootstrap_n):
    df_n = np.empty(0)
    for _ in range(bootstrap_n):
        df_n = np.append(df_n, data.sample(n=n, replace=True).sum())
    return np.mean(np.abs(df_n - np.mean(df_n)))


def kappa(data, n, n0=1, bootstrap_n=1_000_000):
    m_n = M(data, n, bootstrap_n)
    m_n0 = M(data, n0, bootstrap_n)

    return 2 - (np.log(n) - np.log(n0)) / np.log(m_n / m_n0)


def n_v(k_1, n_g):
    return n_g ** (-1 / (k_1 - 1))
