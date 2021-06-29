import sys
import pandas as pd
from kauffman.data import pep
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
    df = pep(obs_level='us')
    print(df.head())
    time_series(df, 'population', 'time', recessions=True)


if __name__ == '__main__':
    _time_series_tests()
