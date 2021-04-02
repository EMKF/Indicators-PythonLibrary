# import kauffman
import numpy as np
import seaborn as sns  # todo: will need to add sklearn to (or maybe just this module) to setup.py
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression  # todo: will need to add sklearn to (or maybe just this module) to setup.py\


# import bfs_distributions.kauffman_tools.public_data_helpers

# def ts_plot(df):
#     df. \
#         astype({'time': 'str'}).\
#         pub.plot(
#             {'BA_BA': 'Firms'},
#             to_index=False,
#             show=True
#         )


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



def sigma_calc(df_ba):
    sigma = (df_ba['BA_BA'].max() - df_ba['BA_BA'].mean()) / df_ba['BA_BA'].std()
    print(f'Number of standard deviations: {sigma}')  # close to 6...It's a six sigma event!

