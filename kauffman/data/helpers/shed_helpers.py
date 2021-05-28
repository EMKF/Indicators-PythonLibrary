import pandas as pd
import constants as c
import sys

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
# pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.options.mode.chained_assignment = None


def transformation_func(df):
    # rename columns to lower case
    df.columns = df.columns.str.lower()
    # subset each df by columns of interest
    df = df[['caseid', 'time', 'ppstaten', 'ppethm', 'ppgender', 'ppage', 'e2']]
    # replace values with state abbreviation
    df["ppstaten"].replace(c.state_codes, inplace=True)
    # replace values with answers
    df["e2"].replace({-1: "refused", 0: "no", 1: "yes"}, inplace=True)
    df['ppethm'].replace({1: "White, Non‐Hispanic", 2: "Black, Non‐Hispanic",\
                          3: "Other, Non‐Hispanic", 4: "Hispanic", 5: "2+ Races, Non‐Hispanic"})
    df['ppgender'].replace({1: "Male", 2: "Female"})
    # make everything lowercase
    df = df.apply(lambda x: x.astype(str).str.lower())
    # Rename columns
    df.rename(columns={"caseid": "id", "ppstaten": "region", "e2": "med_exp_12_months", "ppethm": "race_ethnicity",\
                       "ppage": "age", "ppgender": "gender"}, inplace=True)
    return df

def _shed_data_create():
    return pd.concat(
        [
            c.read_zip(c.shed_dic[year]['zip_url'], c.shed_dic[year]['filename']).\
                assign(time=year).\
                rename(c.shed_dic[year]['col_name_dic']).\
                pipe(transformation_func)
            for year in range(2013, 2021)
        ]
    )


def main():
    df = _shed_data_create()
    print(df.head())
    print(df.tail())
    df.to_csv('/Users/hmurray/Desktop/data/SHED/may_2021/data_create/shed_aggregated_5.21.csv')

if __name__ == '__main__':
    main()
