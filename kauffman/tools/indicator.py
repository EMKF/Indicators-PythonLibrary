import pandas as pd
from .plotting_aids import time_series
from ..data.eship_data_sources import bds, bfs, pep


def _merge_drop(df, indicator, divisor):
    df[f'{indicator}_per_{divisor}'] = df[indicator] / df[divisor]
    return df.drop(divisor, 1)


class Indicator:
    def __init__(self, data):
        self.data = data
        # self.obs_level = None
        self.freq = 'yearly'

    # def _obs_level(self):  # todo: make programmatic and remove parameter in init, if had database, this would be fast
    #     pass

    def rate(self, indicator, divisor):
        if divisor == 'population':
            df_temp = pep(obs_level='us')
        if divisor == 'firms':
            df_temp = bds(series_lst=['FIRM'], obs_level='all').\
                rename(columns={'FIRM': 'firms'}).\
                drop('region', 1)
        if divisor == 'applications':
            df_temp = bfs(series_lst=['BA_BA'], obs_level='us', annualize=True). \
                rename(columns={'BA_BA': 'applications'}).\
                drop('region', 1)

        self.data = self.data.\
            merge(df_temp, how='left', on=['fips', 'time']).\
            pipe(_merge_drop, indicator, divisor)
        return self

    def plot_ts(self, indicator_lst, strata_dic=None, show=True, save_path=None, title=None, to_index=False,
                recessions=False, filter=False, start_year=None, end_year=None, day_marker=None):
        time_series(self.data, indicator_lst, strata_dic, show, save_path, title, to_index, recessions, filter, start_year, end_year, day_marker, frequency=self.freq)
        return self

# how to get the indicator you're interested in?
#   one option is get reading in the data
#   another is manipulating and creating as part of the object
#   for now, I'll just pass in the data
#       index is indentifiers (fips, region, time, etc.)
#       columns are indicators

# indicator attributes that can be...
# 1. plotted
# 2. statisticed
#     a. machine learning prediction
#     b. tails, etc.
#     c. weighted cross-tabs
# 3. changed, like transformed in a rate
