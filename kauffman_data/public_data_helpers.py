import sys
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import kauffman_data.constants as c

def _grouper(df, lvalues):  # todo: should I put this inside the class?
    if lvalues > 1:
        return df. \
            groupby('time').sum(). \
            reset_index()
    return df.reset_index(drop=True)


@pd.api.extensions.register_dataframe_accessor("pub")
class PublicDataHelpers:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        # verify there is a column latitude and a column longitude
        if 'time' not in obj.columns:  # todo: probably want to change this to year
            raise AttributeError("Must have 'time'.")
        # if obj['time'].dtype in ['object', 'str']:
        #     raise AttributeError("'time' is wrong type.")

    # @property
    # def center(self):
    #     # return the geographic center point of this DataFrame
    #     lat = self._obj.latitude
    #     lon = self._obj.longitude
    #     return (float(lon.mean()), float(lat.mean()))

    def econ_indexer(self, var):
        # todo: check if var is None and then is a series and doesn't subset
        df = self._obj.set_index('time')[var]
        initial = df.iloc[0]
        return (((df - initial) / initial) + 1) * 100

    def plot(self, var_lst, strata_dic=None, show=True, save_path=None, title=None, to_index=False, years='All',
             recessions=False):
        """
        var_lst: list or dict
            If dict, the keys are the column names from the dataframe and the values are corresponding descriptions. If
            a list, then the column names are used as the descriptions.
        strata_dic: dict or None
            Dictionary with keys equal to stratifying columns in the dataframe. The values are a dictionary containing
            the stratifying values and their labels as keys.
        """
        df_in = self._obj
        if isinstance(var_lst, list):
            var_label_lst = zip(var_lst, var_lst)
        else:
            var_label_lst = zip(var_lst.keys(), var_lst.values())

        if years == 'All':
            years_lst = list(range(df_in['time'].min(), df_in['time'].max() + 1))
        else:
            years_lst = years

        sns.set_style("whitegrid")  #, {'axes.grid': False})
        fig = plt.figure(figsize=(12, 8))
        for ind, var in enumerate(var_label_lst):
            ax = fig.add_subplot(len(var_lst), 1, ind + 1)

            if strata_dic:
                for strat_var, strat_values_dict in strata_dic.items():
                    for label, values in strat_values_dict.items():
                        df = df_in. \
                            query('{key} in {value}'.format(key=strat_var, value=values)) \
                            [['time', var[0]]]. \
                            pipe(_grouper, len(values)).\
                            query('time in {}'.format(list(years_lst))). \
                            pipe(lambda x: x.pub.econ_indexer(var[0]) if to_index else x.set_index('time')[var[0]])

                        sns.lineplot(data=df, ax=ax, label=label, sort=False)
            else:
                df = df_in \
                    [['time', var[0]]]. \
                    query('time in {}'.format(list(years_lst))). \
                    pipe(lambda x: x.pub.econ_indexer(var[0]) if to_index else x.set_index('time')[var[0]])
                sns.lineplot(x='time', y=var[0], data=df, ax=ax, label=var[1], sort=False)

            if recessions:
                for rec in [(1980, 1980 + (7/12)), (1981 + (7 / 12), 1982 + (11/12)), (1990 + (7 / 12), 1991 + (3/12)), (2001 + (3 / 12), 2001 + (11/12)), (2007 + (12 / 12), 2009 + (6/12))]:
                    if rec[0] >= min(years_lst) and rec[1] <= max(years_lst):
                        ax.axvspan(rec[0], rec[1], alpha=0.3, color='gray')

            ax.set_xlabel(None)
            ax.set_ylabel(var[1])
            ax.legend()

        if title:
            plt.title(title)
        if save_path:
            plt.savefig(save_path)
        if show:
            plt.show()
