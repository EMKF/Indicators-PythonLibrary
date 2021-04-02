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
        # ax.text(df[outcome].quantile(.995), 1.1, f'$\\alpha$ = {round(a, 2)}', color='firebrick')

    plt.title(f'Log-Log Plot of {outcome_label}: $\\alpha == {round(a, 2)}$')
    plt.show()


def ms_plot(df, p):
    n = df.shape[0]
    ms_lst = []
    for num in range(1, n + 1):
        df_temp = df['BA_BA'].iloc[0:num] ** p
        S = df_temp.sum()
        M = df_temp.max()
        ms_lst.append(M / S)

    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(range(n), ms_lst)
    ax.set_xlabel('n')
    ax.set_ylabel(f'MS({p})')
    plt.show()
#     what does this tell us?

def excess_conditional_expectation(df):
    '''Test 2 in chapter 10, page 190'''
    k_lst = np.linspace(df['BA_BA'].min(), df['BA_BA'].max(), 1000)

    ce_lst = [df.query(f'BA_BA > {k}')['BA_BA'].mean() / k for k in k_lst]

    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(k_lst, ce_lst)
    ax.set_xlabel('K')
    ax.set_ylabel('$\\frac{E(X) |_{X>K}}{K}$')
    plt.show()





def sigma_calc(df_ba):
    sigma = (df_ba['BA_BA'].max() - df_ba['BA_BA'].mean()) / df_ba['BA_BA'].std()
    print(f'Number of standard deviations: {sigma}')  # close to 6...It's a six sigma event!

