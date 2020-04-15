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

def _year_lst(df, v):
    year_lst = [col.split('_')[1] for col in df.columns.tolist() if v in col]
    # year_lst.sort()
    print(year_lst); sys.exit()
    return

def _state_transform(df, v, df_state):
    covar_lst = [col for col in df.columns.tolist() if v in col]
    new_covar_lst = [col.replace('_', '') for col in covar_lst]
    df_v = df.\
        rename(columns={**dict(zip(covar_lst, new_covar_lst)), **{'sname': 'name'}}) \
        [['name'] + new_covar_lst].\
        pipe(pd.wide_to_long, v, i='name', j='year').\
        reset_index() \
        [['name', 'year', v]]
    if df_state.shape[1] == 0:
        return df_v
    return df_state.merge(df_v, how='left', on=['name', 'year'])


def _us_transform(df, v, df_us):
    covar_lst = [col for col in df.columns.tolist() if v in col and col.count('_') == 1]
    new_covar_lst = [col.replace('_', '') for col in covar_lst]
    df_v = df.\
        rename(columns={**dict(zip(covar_lst, new_covar_lst)), **{'sname': 'name', 'demtype': 'type', 'demographic': 'category'}}). \
        assign(
            type=lambda x: 'Total' if 'type' not in x.columns else x['type'],
            category=lambda x: 'Total' if 'category' not in x.columns else x['category'],
        ) \
        [['name', 'type', 'category'] + new_covar_lst].\
        pipe(pd.wide_to_long, v, i=['name', 'type', 'category'], j='year').\
        reset_index()

    # print(df_v)

    if df_us.shape[1] == 0:
        return df_v
    return df_us.merge(df_v, how='left', on=['name', 'type', 'category', 'year'])


    print(df_v)
    sys.exit()
    # print(v)

    covar_lst = [col for col in df.columns.tolist() if v in col]
    new_covar_lst = [col.replace('_', '') for col in covar_lst]

    df_group = df. \
        rename(columns={**dict(zip(covar_lst, new_covar_lst)), **{'sname': 'name'}}). \
        assign(
            demtype=lambda x: 'overall' if 'demtype' not in x.columns else x['demtype'],
            demographic=lambda x: 'overall' if 'demographic' not in x.columns else x['demographic']
        ) \
        [['name', 'demtype', 'demographic'] + new_covar_lst]. \
        groupby(['name', 'demtype'])

    df_out = pd.DataFrame()
    for group in df_group:

        df_temp = pd.wide_to_long(group[1], v, i=['name', 'demtype', 'demographic'], j='year').\
            reset_index().\
            rename(columns={'demographic': group[0][1]}).\
            drop('demtype', 1)
        df_out = df_out.append(df_temp)

    #         fillna('economy wide'). \
    df_v = df_out. \
        assign(
            Age=lambda x: 'economy wide' if 'Age' not in x.columns else x['Age'],
            Education=lambda x: 'economy wide' if 'Education' not in x.columns else x['Education'],
            Nativity=lambda x: 'economy wide' if 'Nativity' not in x.columns else x['Nativity'],
            Race=lambda x: 'economy wide' if 'Race' not in x.columns else x['Race'],
            Sex=lambda x: 'economy wide' if 'Sex' not in x.columns else x['Sex'],
        ) \
        [['name', 'year', 'Age', 'Education', 'Nativity', 'Race', 'Sex'] + [v]]
    # todo: nans are being filled in as economy wides
    print(df_v)
    if df_us.shape[1] == 0:
        return df_v
    return df_us.merge(df_v, how='left', on=['name', 'year', 'Age', 'Education', 'Nativity', 'Race', 'Sex'])


def fairlie_to_panel(state_file_path, us_file_path):
    """
    file_path_lst: lst
        List containing file paths of Excel file with the state-level and national-level KESE data.
    """
    indicator_dict = dict(
        zip(
            ['Rate of New Entrepreneurs', 'Opportunity Share of NE', 'Startup Job Creation', 'Startup Survival Rate',
             'KESE Index'],
            ['rne', 'ose', 'sjc', 'ssr', 'zindex']
        )
    )

    df_us, df_state = pd.DataFrame(), pd.DataFrame()
    for k, v in indicator_dict.items():
        df_state = _state_transform(pd.read_excel(state_file_path, sheet_name=k), v, df_state)
        df_us = _us_transform(pd.read_excel(us_file_path, sheet_name=k), v, df_us)
    print(df_us)
    sys.exit()


    #
    # df_state.\
    #     append(df_us).\
    #     reset_index(drop=True).\
    #     pipe(_file_saver, v)
    #




    for i, indicators in enumerate(indicator_dict.values()):
        df_temp = joblib.load(c.filenamer('data/kese_{}.pkl'.format(indicators))). \
            rename(
            columns={
                **dict(zip(range(1996, end_year),
                           map(lambda x: '{0}{1}'.format(indicators, x), range(1996, end_year)))),
                **{'region': 'fips'}
            }
        ). \
            assign(name=lambda x: x['fips'].map(c.state_codes).map(c.abbrev_us_state)). \
            pipe(pd.wide_to_long, [indicators], i=['name', 'fips', 'demographic-type', 'demographic'], j='year'). \
            reset_index(). \
            replace('', 'overall')
        if i == 0:
            df = df_temp
        else:
            df = df.merge(df_temp, how='outer', on=['name', 'fips', 'demographic-type', 'demographic', 'year'])

    df = df.assign(sex='', race='', nativity='', age='', education='', veteran_status='')

    df.loc[
        df['demographic-type'] == 'overall', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Male', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'male', 'overall', 'overall', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Female', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'female', 'overall', 'overall', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'White', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'white', 'overall', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Black', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'black', 'overall', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Latino', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'latino', 'overall', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Asian', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'asian', 'overall', 'overall', 'overall', 'overall']
    df.loc[
        df['demographic'] == 'Native-Born', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'native_born', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Immigrant', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'immigrant', 'overall', 'overall', 'overall']
    df.loc[df['demographic'] == 'Ages 20-34', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'ages 20-34', 'overall', 'overall']
    df.loc[df['demographic'] == 'Ages 35-44', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'ages 35-44', 'overall', 'overall']
    df.loc[df['demographic'] == 'Ages 45-54', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'ages 45-54', 'overall', 'overall']
    df.loc[df['demographic'] == 'Ages 55-64', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'ages 55-64', 'overall', 'overall']
    df.loc[df['demographic'] == 'Less than High School', ['sex', 'race', 'nativity', 'age', 'education',
                                                          'veteran_status']] = ['overall', 'overall', 'overall',
                                                                                'overall', 'less than high school',
                                                                                'overall']
    df.loc[df['demographic'] == 'High School Graduate', ['sex', 'race', 'nativity', 'age', 'education',
                                                         'veteran_status']] = ['overall', 'overall', 'overall',
                                                                               'overall', 'high school graduate',
                                                                               'overall']
    df.loc[
        df['demographic'] == 'Some College', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'overall', 'some college', 'overall']
    df.loc[df['demographic'] == 'College Graduate', ['sex', 'race', 'nativity', 'age', 'education',
                                                     'veteran_status']] = ['overall', 'overall', 'overall',
                                                                           'overall', 'college graduate', 'overall']
    df.loc[df['demographic'] == 'Veterans', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'overall', 'overall', 'veterans']
    df.loc[
        df['demographic'] == 'Non-Veterans', ['sex', 'race', 'nativity', 'age', 'education', 'veteran_status']] = [
        'overall', 'overall', 'overall', 'overall', 'overall', 'non-veterans']

    return df. \
        drop(['demographic-type', 'demographic'], 1). \
        sort_values(['fips', 'year']). \
        reset_index(drop=True)  #. \
        # to_csv(c.filenamer('data/kese_download.csv'), index=False)


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


    # todo: does this work with multiple covariates? It works with Age of Business from jobs.
    def panel_to_alley(self, covar_lst, outcome):
        """
        covar_lst: lst
            List of covariates that are used to stratify the data.
        outcome: str
            The column name of the outcome whose values become the cells of the dataframe.
        """
        data_in = self._obj
        self._validate_panel(data_in, covar_lst)
        year_lst = data_in['year'].unique().tolist()

        df = data_in[['fips', 'year'] + covar_lst + [outcome]].\
            pipe(pd.pivot_table, index=['fips'] + covar_lst, columns='year', values=outcome).\
            reset_index().\
            replace('overall', '').\
            assign(demographic=lambda x: x[covar_lst].agg(''.join, axis=1))

        covar_dict = {}
        for covar in covar_lst:
            for val in df[covar].unique():
                covar_dict[val] = covar
        df.loc[:, 'demographic-type'] = df['demographic'].map(covar_dict)
        df.loc[df['demographic'] == '', 'demographic-type'] = ''

        return df[['fips', 'demographic-type', 'demographic'] + year_lst]


    def econ_indexer(self, var):
        # todo: check if var is None and then is a series and doesn't subset
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
            offset = {'quarters': 1}
        else: # if yearly
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
