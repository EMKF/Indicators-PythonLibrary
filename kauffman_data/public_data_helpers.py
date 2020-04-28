import sys
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
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
        # self._validate(pandas_obj)
        self._obj = pandas_obj
        self.covars = pandas_obj.columns.tolist()

    @staticmethod
    def _validate(obj):
        # verify there is a column latitude and a column longitude
        if 'time' not in obj.columns:  # todo: probably want to change this to year
            raise AttributeError("Must have 'time'.")

        # if obj['time'].dtype in ['object', 'str']:
        #     raise AttributeError("'time' is wrong type.")

    @staticmethod
    def _validate_panel(obj, covar_lst):
        if ('year' not in obj.columns):  # todo: probably want to change this to year
            raise KeyError("Must have a column named 'year' in your dataframe.")
        if ('fips' not in obj.columns):
            raise KeyError("Must have a column named 'fips' in your dataframe.")
        if not isinstance(covar_lst, list):
            raise AttributeError("covar_lst must be a list.")

    # @property
    # def center(self):
    #     # return the geographic center point of this DataFrame
    #     lat = self._obj.latitude
    #     lon = self._obj.longitude
    #     return (float(lon.mean()), float(lat.mean()))


    def download_to_alley_formatter(self, covar_lst, outcome):
        """
        covar_lst: lst
            List of covariates that are used to stratify the data.
        outcome: str
            The column name of the outcome whose values become the cells of the dataframe.
        """
        data_in = self._obj
        self._validate_panel(data_in, covar_lst)

        return data_in[['fips', 'year'] + covar_lst + [outcome]].\
            pipe(pd.pivot_table, index=['fips'] + covar_lst, columns='year', values=outcome).\
            reset_index().\
            replace('Total', '').\
            rename(columns={'type': 'demographic-type', 'category': 'demographic'})

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

    def plot(self, var_lst=None, strata_dic=None, show=True, save_path=None, title=None, to_index=False,
             recessions=False, filter=False, start_year=None, end_year=None, day_marker=None):
        """
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

        df_in = self._obj
        if not var_lst:
            var_lst = [var for var in df_in.columns if var not in ['time', 'region']]


        if df_in.loc[0, 'time'].count('-') == 2:  # if weekly
            lamb = 1600  # hmmm
            # lamb = 45697600
            df_in = self._obj.assign(time=lambda x: pd.to_datetime(x['time'].astype(str)))
            offset = {'weeks': 1}
        elif df_in.loc[0, 'time'].count('-') == 1:  # if monthly
            lamb = 129600
            df_in = self._obj.assign(time=lambda x: pd.to_datetime(x['time'].astype(str)))
            offset = {'months': 1}
        elif 'Q' in df_in.loc[0, 'time']:  # if quarterly
            lamb = 1600
            df_in = self._obj.assign(time=lambda x: pd.to_datetime(x['time'].astype(str)))
            offset = {'months': 3}
        else:  # if yearly
            lamb = 6.25
            df_in = df_in.assign(time=lambda x: pd.to_datetime(x['time'].astype(str) + '-07'))
            offset = {'years': 1}


        if isinstance(var_lst, list):  # if var_lst is a list and not a string
            var_label_lst = zip(var_lst, var_lst)
        else:
            var_label_lst = zip(var_lst.keys(), var_lst.values())
        if not start_year:  # if start_year is specified
            start_year = df_in.loc[0, 'time']
        else:
            start_year = pd.to_datetime(str(start_year))
        if not end_year:  # if end_year is specified
            end_year = df_in.iloc[-1, :]['time']
        else:
            end_year = pd.Timestamp.now()

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
                            query('time >= "{}"'.format(start_year)). \
                            query('time <= "{}"'.format(end_year)). \
                            query('{var} == {var}'.format(var=var[0])). \
                            pipe(lambda x: x.pub.econ_indexer(var[0]) if to_index else x.set_index('time')[var[0]])
                        sns.lineplot(data=df, ax=ax, label=label, sort=False)

                        if filter:
                            cycle, trend = sm.tsa.filters.hpfilter(df, lamb=lamb)
                            ax.lines[-1].set_linestyle("--")
                            ax.lines[-1]._alpha = .5
                            sns.lineplot(data=trend, ax=ax, sort=False, color=ax.lines[-1].get_color())

            else:
                df = df_in \
                    [['time', var[0]]]. \
                    assign(time=lambda x: pd.to_datetime(x['time'])).\
                    query('time >= "{}"'.format(start_year)). \
                    query('time <= "{}"'.format(end_year)). \
                    query('{var} == {var}'.format(var=var[0])). \
                    pipe(lambda x: x.pub.econ_indexer(var[0]) if to_index else x.set_index('time')[var[0]])
                sns.lineplot(data=df, ax=ax, label=var[1], sort=False)

                if filter:
                    cycle, trend = sm.tsa.filters.hpfilter(df, lamb=lamb)
                    ax.lines[-1].set_linestyle("--")
                    ax.lines[-1]._alpha = .5
                    sns.lineplot(data=trend, ax=ax, sort=False, color=ax.lines[-1].get_color())

            if recessions:
                recession_dates = [
                    (pd.to_datetime('1948-11'), pd.to_datetime('1949-10')),
                    (pd.to_datetime('1953-07'), pd.to_datetime('1954-05')),
                    (pd.to_datetime('1957-08'), pd.to_datetime('1958-04')),
                    (pd.to_datetime('1960-04'), pd.to_datetime('1961-02')),
                    (pd.to_datetime('1969-12'), pd.to_datetime('1970-11')),
                    (pd.to_datetime('1973-11'), pd.to_datetime('1975-03')),
                    (pd.to_datetime('1980-01'), pd.to_datetime('1980-07')),
                    (pd.to_datetime('1981-07'), pd.to_datetime('1982-11')),
                    (pd.to_datetime('1990-07'), pd.to_datetime('1991-03')),
                    (pd.to_datetime('2001-03'), pd.to_datetime('2001-11')),
                    (pd.to_datetime('2007-12'), pd.to_datetime('2009-06'))
                ]
                for rec in recession_dates:
                    if rec[0] >= start_year and rec[1] <= end_year:
                        ax.axvspan(rec[0], rec[1], alpha=0.3, color='gray')

            if day_marker:
                first = True
                for df_date in df.index:
                    marker = '{year}-{month_day}'.format(year=df_date.year, month_day=day_marker)
                    if df_date < pd.to_datetime(marker) and df_date + pd.DateOffset(**offset) >= pd.to_datetime(marker):
                        if first:
                            ax.axvspan(df_date, df_date + pd.DateOffset(**offset), alpha=0.3, color='firebrick', label='Week of {}'.format(day_marker))
                            first=False
                        else:
                            ax.axvspan(df_date, df_date + pd.DateOffset(**offset), alpha=0.3, color='firebrick')


            ax.set_xlabel(None)
            ax.set_ylabel(var[1] if not to_index else 'Index: {}'.format(var[1]))
            ax.set_xlim([start_year - pd.DateOffset(**offset), end_year + pd.DateOffset(**offset)])
            ax.legend()

        if filter:
            plt.figtext(0.01, 0.01, 'Note: Dotted lines indicate actual values; solid lines are values that have been smoothed with an HP filter.', horizontalalignment='left', fontsize=8)
        if title:
            plt.title(title)
        if save_path:
            plt.savefig(save_path)
        if show:
            plt.show()
