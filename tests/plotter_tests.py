import sys
import pandas as pd
import seaborn as sns
from kauffman.data import pep
import matplotlib.pyplot as plt
from kauffman.plotter import choropleth, time_series


def _choropleth_state():
    df = pd.read_csv('/Users/thowe/Projects/downwardata/tests/data/pep_state.csv').\
        query('time == 2019').\
        drop(['Unnamed: 0', 'region', 'time'], 1).\
        query('fips not in [2, 15]')
    choropleth(
        df,
        'population',
        'state',
        [df.population.min(), df.population.max()],
        'Population 2019',
        'Source: Census',
        us_map=48
    )

def _choropleth_county():
    df = pd.read_csv('/Users/thowe/Projects/downwardata/tests/data/pep_county.csv').\
        query('time == 2019').\
        drop(['region', 'time'], 1).\
        assign(
            fips=lambda x: x['fips'].apply(lambda y: '0' + str(y) if len(str(y)) == 4 else str(y)),
            state=lambda x: x['fips'].apply(lambda y: '0' + str(y)[0] if len(str(y)) == 4 else str(y)[:2])
        ).\
        query('state not in ["02", "15"]').\
        query('population > 100_000')
    choropleth(
        df,
        'population',
        'county',
        [df.population.min(), df.population.max()],
        'Population 2019',
        'Source: Census',
        us_map=48
    )


def _choropleth_msa():
    df = pd.read_csv('/Users/thowe/Projects/downwardata/tests/data/pep_msa.csv').\
        query('time == 2019').\
        astype({'fips': 'str'}).\
        query('population > 1_000_000')
    choropleth(
        df,
        'population',
        'msa',
        [df.population.min(), df.population.max()],
        'Population 2019',
        'Source: Census',
        us_map=48
    )


def _time_series_tests():
    # df = pep(obs_level='us')
    # print(df.head())
    # time_series(df, 'population', 'time', recessions=True)

    industries = ['Health Care and Social Assistance', 'Retail Trade', 'Construction', 'Professional, Scientific, and Technical Services', 'Administrative and Support and Waste Management and Remediation Services']
    df = pd.read_csv(f'/Users/thowe/Downloads/mpj_heartland_industry.csv')  #.\
        # query(f'industry in {industries}')

    for state in ['Missouri', 'Kansas', 'Nebraska', 'Iowa']:
        df_temp = df.query(f'region == "{state}"')

        ind_dict = {
            'Agriculture, Forestry, Fishing and Hunting': ['black', 'solid'],
            'Mining, Quarrying, and Oil and Gas Extraction': ['lightgray', 'solid'],
            'Utilities': ['red', 'solid'],
            'Construction': ['orange', 'solid'],
            'Manufacturing': ['green', 'solid'],
            'Wholesale Trade': ['blue', 'solid'],
            'Retail Trade': ['violet', 'solid'],
            'Information': ['black', 'dotted'],
            'Finance and Insurance': ['lightgray', 'dotted'],
            'Real Estate and Rental and Leasing': ['red', 'dotted'],
            'Professional, Scientific, and Technical Services': ['orange', 'dotted'],
            'Management of Companies and Enterprises': ['green', 'dotted'],
            'Administrative and Support and Waste Management and Remediation Services': ['blue', 'dotted'],
            'Educational Services': ['violet', 'dotted'],
            'Health Care and Social Assistance': ['black', 'dashed'],
            'Arts, Entertainment, and Recreation': ['lightgray', 'dashed'],
            'Accommodation and Food Services': ['red', 'dashed'],
            'Other Services (except Public Administration)': ['orange', 'dashed']
         }

        sns.set_style("whitegrid")
        sns.color_palette('Paired')
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(1, 1, 1)
        ax.set_ylim([0, .23])
        for k, v in ind_dict.items():
            df_temp_ind = df_temp.query(f'industry == "{k}"')
            ax.plot(df_temp_ind['time'], df_temp_ind['contribution'], color=v[0], linestyle=v[1], label=k)
            # sns.lineplot(x='time', y='contribution', data=df_temp.query(f'industry == "{k}"'), color=v[0], style=v[1], ax=ax, label=k)
        # sns.lineplot(x='time', y='contribution', data=df_temp, hue='industry', markers='industry', ax=ax, palette='tab10')
        # sns.lineplot(x='time', y='contribution', data=df_temp, size='industry', markers='industry', ax=ax, palette='tab10')

        plt.legend(loc='upper left')
        plt.title(f'{state}')
        plt.savefig(f'/Users/thowe/Downloads/mpj_industry_{state}.png')
        plt.show()
    sys.exit()


    # time_series(df, ['contribution', 'constancy', 'creation'], 'time')  # this doesn't work

    # df.pub.plot(
    #     {'firms': 'Firms'},
    #     strata_dic={'age': {'Startups': ['startup'], 'Non-startups': ['non-startup']}},
    #     to_index=True,
    #     recessions=True,
    #     filter=True,
    #     save_path=c.filenamer('bds/data/su_nsu_firms_indexed.png'),
    #     show=True
    # )


if __name__ == '__main__':
    _time_series_tests()
