import pandas as pd
import kauffman.constants as c
from ..eship_data_sources import pep
from kauffman.tools.etl import read_zip

def _col_names_lowercase(df):
    df.columns = df.columns.str.lower()
    return df


def _shed_data_create():
    return pd.concat(
        [
            read_zip(c.shed_year_to_info[year]['zip_url'], c.shed_year_to_info[year]['filename']). \
                pipe(_col_names_lowercase).\
                assign(
                    time=year,
                    fips=lambda x: x['ppstaten'].map(c.shed_state_code_to_abb).map(c.state_abb_to_fips),
                    region=lambda x: x['ppstaten'].map(c.shed_state_code_to_abb).map(c.state_abb_to_name),
                    e2=lambda x: x['e2'].map({-1: "refused", 0: "no", 1: "yes"}),
                    ppethm=lambda x: x['ppethm'].map({1: "White, Non‐Hispanic", 2: "Black, Non‐Hispanic", 3: "Other, Non‐Hispanic", 4: "Hispanic", 5: "2+ Races, Non‐Hispanic"}),
                    ppgender=lambda x: x['ppgender'].map({1: "Male", 2: "Female"})
                ). \
                rename(columns={"caseid": "id", "e2": "med_exp_12_months", "ppethm": "race_ethnicity", "ppage": "age", "ppgender": "gender"}) \
                [['id', 'time', 'fips', 'region', 'med_exp_12_months', 'race_ethnicity', 'age', 'gender']]
            for year in range(2013, 2021)
        ]
    )
