import sys
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
import matplotlib.pyplot as plt
import kauffman.constants as c


def _grouper(df, lvalues):  # todo: should I put this inside the class?
    if lvalues > 1:
        return df. \
            groupby('time').sum(). \
            reset_index()
    return df.reset_index(drop=True)


def econ_indexer(self, var):
    """
    var: str or lst
        if lst, then returns a dataframe with all columns in the list transformed into an index
    """
    # todo: check if var is None and then is a series and doesn't subset

    if isinstance(var, list):
        df = self._obj
        for col in var:
            initial = df.iloc[0][col]
            df.loc[:, col] = 100 * (((df[col] - initial) / initial) + 1)
        return df
    else:
        df = self._obj.set_index('time')[var]
        initial = df.iloc[0]
        return (((df - initial) / initial) + 1) * 100


def time_series(df, var_lst, time, strata_dic=None, show=True, save_path=None, title=None, to_index=False,
         recessions=False, filter_lambda=None, day_marker=None):
    """
    df: pandas df
    var_lst: str, list or dict representing the outcome variable(s) to plot
    time: str reprensenting the time variable
        todo how should this be formatted?
        think it needs to be a datetime

    filter_lambda: None or int
        int is smoothing lambda
        Common lambda values are
            weekly: 1600
            monthly: 129600
            quarterly: 1600
            yearly: 6.25



    var_lst: list or dict
        If dict, the keys are the column names from the dataframe and the values are corresponding descriptions. If
        a list, then the column names are used as the descriptions. These are the covariates from the dataframe you
        want to plot. The time variable should be called 'time'.
    strata_dic: dict or None
        Dictionary with keys equal to stratifying columns in the dataframe. The values are a dictionary containing
        the stratifying values and their labels as keys.
    years: str or list
        If 'All' then use all available data. Otherwise, subset the data based on the years specified.
    start_year: int
        First year of data to use.
    """


    df = df.assign(_time=lambda x: pd.to_datetime(x[time].astype(str)))

    start_year = pd.to_datetime(df['_time'].dt.year.min(), format='%Y')
    end_year = pd.to_datetime(df['_time'].dt.year.max(), format='%Y')

    if isinstance(var_lst, str):  # if var_lst is a str
        var_lst = [var_lst]
        var_label_lst = zip(var_lst, var_lst)
    elif isinstance(var_lst, list):  # if var_lst is a list
        var_label_lst = zip(var_lst, var_lst)
    else:
        var_label_lst = zip(var_lst.keys(), var_lst.values())

    sns.set_style("whitegrid")  # , {'axes.grid': False})
    fig = plt.figure(figsize=(12, 8))
    for ind, var in enumerate(var_label_lst):
        ax = fig.add_subplot(len(var_lst), 1, ind + 1)

        if strata_dic:
            for strat_var, strat_values_dict in strata_dic.items():
                for label, values in strat_values_dict.items():
                    df = df. \
                        query('{key} in {value}'.format(key=strat_var, value=values)) \
                        [['time', var[0]]]. \
                        pipe(_grouper, len(values)). \
                        query('{var} == {var}'.format(var=var[0])). \
                        pipe(lambda x: x.pub.econ_indexer(var[0]) if to_index else x.set_index('time')[var[0]])
                    sns.lineplot(data=df, ax=ax, label=label, sort=False)

                    if filter_lambda:
                        cycle, trend = sm.tsa.filters.hpfilter(df, lamb=filter_lambda)
                        ax.lines[-1].set_linestyle("--")
                        ax.lines[-1]._alpha = .5
                        sns.lineplot(data=trend, ax=ax, sort=False, color=ax.lines[-1].get_color())

        else:
            df = df \
                [['_time', var[0]]]. \
                query('{var} == {var}'.format(var=var[0])). \
                pipe(lambda x: econ_indexer(var[0]) if to_index else x.set_index('_time')[var[0]])

            sns.lineplot(data=df, ax=ax, label=var[1], sort=False)

            if filter_lambda:
                cycle, trend = sm.tsa.filters.hpfilter(df, lamb=filter_lambda)
                ax.lines[-1].set_linestyle("--")
                ax.lines[-1]._alpha = .5
                sns.lineplot(data=trend, ax=ax, sort=False, color=ax.lines[-1].get_color())

        if recessions:
            for rec in c.recession_dates:
                if start_year <= rec[0] and rec[1] <= end_year:
                    ax.axvspan(rec[0], rec[1], alpha=0.3, color='gray')

        # if day_marker:
        #     first = True
        #     for df_date in df.index:
        #         marker = '{year}-{month_day}'.format(year=df_date.year, month_day=day_marker)
        #         if df_date < pd.to_datetime(marker) and df_date + pd.DateOffset(**offset) >= pd.to_datetime(marker):
        #             if first:
        #                 ax.axvspan(df_date, df_date + pd.DateOffset(**offset), alpha=0.3, color='firebrick',
        #                            label='Week of {}'.format(day_marker))
        #                 first = False
        #             else:
        #                 ax.axvspan(df_date, df_date + pd.DateOffset(**offset), alpha=0.3, color='firebrick')

        ax.set_xlabel(None)
        ax.set_ylabel(var[1] if not to_index else 'Index: {}'.format(var[1]))
        # ax.set_xlim([start_year - pd.DateOffset(**offset), end_year + pd.DateOffset(**offset)])
        # ax.set_xlim([start_year - pd.DateOffset(**offset), end_year + pd.DateOffset(**offset)])
        ax.legend()


    # todo: note for recessions being grey shaded areas...somehow combined with below
    if filter:
        plt.figtext(0.01, 0.01,
                    'Note: Dotted lines indicate actual values; solid lines are values that have been smoothed with an HP filter.',
                    horizontalalignment='left', fontsize=8)
    if title:
        plt.title(title)
    if save_path:
        plt.savefig(save_path)
    if show:
        plt.show()
