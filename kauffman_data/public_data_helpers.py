import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import kauffman_data.constants as c

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

    def plot(self, var_lst, strata_dic=None, show=True, save_path=None):
        """
        var_lst: list or dict
            If dict, the keys are the column names from the dataframe and the values are corresponding descriptions. If
            a list, then the column names are used as the descriptions.
        strata_dic: dict or None
            Dictionary with keys equal to stratifying columns in the dataframe. The values are the stratifying values.
        """

        df = self._obj
        if strata_dic:
            for pair in zip(strata_dic.keys(), strata_dic.values()):
                df.query('{key} == "{value}"'.format(key=pair[0], value=pair[1]), inplace=True)
            df.reset_index(inplace=True, drop=True)
        if isinstance(var_lst, list):
            var_label_lst = zip(var_lst, var_lst)
        else:
            var_label_lst = zip(var_lst.keys(), var_lst.values())

        sns.set_style("whitegrid")  #, {'axes.grid': False})
        fig = plt.figure(figsize=(12, 8))
        for ind, var in enumerate(var_label_lst):
            ax = fig.add_subplot(len(var_lst), 1, ind + 1)
            sns.lineplot(x='time', y=var[0], data=df, ax=ax, label=var[1], sort=False)
            ax.set_xlabel(None)
            ax.set_ylabel(var[1])
            ax.legend(loc='upper left')

        # plt.title('Number of startups and the unemployment rate ($r = {}$)'.format(round(r, 2)))
        if save_path:
            plt.savefig(save_path)
        if show:
            plt.show()

