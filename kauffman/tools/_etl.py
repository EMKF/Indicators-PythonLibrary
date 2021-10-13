import io
import sys
import boto3
import requests
import numpy as np
import pandas as pd
from kauffman import constants as c
from zipfile import ZipFile
from itertools import product
import kauffman.constants as c


def file_to_s3(file, s3_bucket, s3_file):
    """
    Upload a local file to S3.

    file: str
        location of the local file
    s3_bucket: str
        Name of the S3 bucket. E.g., 'emkf.data.research'
    s3_file: str
        Destination file location in S3. E.g., 'indicators/nej/data_outputs/your_pickle_filename.pkl'

    Examples:
        from kauffman.data import file_to_s3
        file_to_s3('local_file_path.csv', 'emkf.data.research', 'indicators/nej/data_outputs/remote_file_path.csv')
    """
    s3 = boto3.client('s3')
    with open(file, "rb") as f:
        s3.upload_fileobj(f, s3_bucket, s3_file)


def file_from_s3(file, bucket, key):
    """
    Download a file locally from S3.

    file: str
        Local file destination
    s3_bucket: str
        Name of the S3 bucket where file is located. E.g., 'emkf.data.research'
    s3_file: str
        File location in S3. E.g., 'indicators/nej/data_outputs/your_pickle_filename.pkl'

    Examples:
        from kauffman.data import file_from_s3
        file_from_s3('local_file_path.csv', 'emkf.data.research', 'indicators/nej/data_outputs/remote_file_path.csv')
    """
    s3 = boto3.client('s3')
    s3.download_fileobj(bucket, key, file)


def read_zip(zip_url, filename):
    """
    Reads a csv file from a zip file online. Example: 'public2018.csv' from
    'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip'
    """
    z = ZipFile(io.BytesIO(requests.get(zip_url).content))
    return pd.read_csv(z.open(filename), encoding='cp1252', low_memory=False)


def county_msa_cross_walk(df_county, fips_county, outcomes, agg_method=sum):
    """
    Receives county level data, and merges (on county fips) with a dataframe with CBSA codes.

    fips_county: fips column name
    """
    outcomes = [outcomes] if type(outcomes) == str else outcomes
    df_county[outcomes] = df_county[outcomes].apply(pd.to_numeric)

    df_cw = pd.read_excel(
            'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
            header=2,
            skipfooter=4,
            usecols=[0, 3, 9, 10],
            # usecols=[0, 3, 4, 7, 8, 9, 10],
            converters={
                'FIPS State Code': lambda x: str(x) if len(str(x)) == 2 else f'0{x}',
                'FIPS County Code': lambda x: str(x) if len(str(x)) == 3 else f'0{x}' if len(str(x)) == 2 else f'00{x}',
            }
        ).\
        assign(fips_county=lambda x: x['FIPS State Code'] + x['FIPS County Code']).\
        rename(columns={'CBSA Code': 'fips_msa'}).\
        astype({'fips_msa': 'str'}).\
        drop(['FIPS State Code', 'FIPS County Code'], 1)

    return df_county.\
        rename(columns={fips_county: 'fips_county'}).\
        merge(df_cw, how='left', on='fips_county').\
        drop(['fips_county', 'region'], 1) \
        [['fips_msa', 'CBSA Title', 'time'] + outcomes].\
        groupby(['fips_msa', 'CBSA Title', 'time']).agg(agg_method).\
        reset_index(drop=False).\
        rename(columns={'CBSA Title': 'region', 'fips_msa':'fips'})
        # query('fips == fips'). \
# todo: I can't just groupby and sum wrt cw(), since there might be missing county values


def state_msa_cross_walk(state_lst, area_type='metro'):
    """
    state_lst: list of states

    area_type: str
        metro: default, metro areas
        micro: all micro areas
        all: all micro and metro areas

    output: dataframe with county and msa fips
    """

    if area_type == 'metro':
        area_type = 'Metropolitan Statistical Area'
    elif area_type == 'micro':
        area_type = 'Micropolitan Statistical Area'

    df_cw = pd.read_excel(
            'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
            header=2,
            skipfooter=4,
            usecols=[0, 3, 4, 9, 10],
            converters={
                'FIPS State Code': lambda x: str(x) if len(str(x)) == 2 else f'0{x}',
                'FIPS County Code': lambda x: str(x) if len(str(x)) == 3 else f'0{x}' if len(str(x)) == 2 else f'00{x}',
            }
        ).\
        rename(
            columns={
                'FIPS State Code': 'fips_state',
                'FIPS County Code': 'county_fips',
                'Metropolitan/Micropolitan Statistical Area': 'area',
                'CBSA Code': 'fips_msa'
            }
        ).\
        pipe(lambda x: x if area_type == 'all' else x.query(f'area == "{area_type}"'))

    return df_cw.\
        query(f'fips_state in {state_lst}').\
        drop_duplicates('fips_msa') \
        [['fips_msa']].\
        merge(df_cw, how='left', on='fips_msa').\
        assign(fips_county=lambda x: x['fips_state'] + x['county_fips']).\
        drop(['CBSA Title', 'area', 'county_fips'], 1)


def fips_state_cross_walk(fips_lst, region):
    """
    state_lst: list of states

    area_type: str
        metro: default, metro areas
        micro: all micro areas
        all: all micro and metro areas

    output: dataframe with county and msa fips
    """

    df_cw = pd.read_excel(
            'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
            header=2,
            skipfooter=4,
            usecols=[0, 3, 4, 9, 10],
            converters={
                'FIPS State Code': lambda x: str(x) if len(str(x)) == 2 else f'0{x}',
                'FIPS County Code': lambda x: str(x) if len(str(x)) == 3 else f'0{x}' if len(str(x)) == 2 else f'00{x}',
            }
        ).\
        rename(
            columns={
                'FIPS State Code': 'fips_state',
                'FIPS County Code': 'fips_county',
                'Metropolitan/Micropolitan Statistical Area': 'area',
                'CBSA Code': 'fips_msa'
            }
        ). \
        assign(fips_county = lambda x: x['fips_state'] + x['fips_county']).\
        astype({'fips_msa': 'str'})

    return df_cw.\
        query(f'fips_{region} in {fips_lst}').\
        drop_duplicates([f'fips_{region}', 'fips_state']) \
        [['fips_state', f'fips_{region}']]


def _hispanic_create(df, covars):
    return df.groupby(covars + ['ethnicity']).sum(). \
        reset_index(drop=False). \
        query('ethnicity == "A2"'). \
        assign(race_ethnicity='Hispanic')


def weighted_sum(df, strata = [], var_list = 'all', weight_var=None):
    strata = [strata] if type(strata) == str else strata
    if var_list == 'all':
        var_list = [x for x in df.columns if x not in strata]
    if weight_var == None:
        weight_var = 'weight'
        df['weight'] = 1
    
    return df[strata + var_list].\
        apply(lambda x: x*df[weight_var] if x.name in var_list else x).\
        groupby(strata).sum().\
        reset_index()


def race_ethnicity_categories_create(df, covars):
    return df.\
        query('ethnicity != "A2"').\
        assign(race_ethnicity=lambda x: x['race'].map(c.mpj_covar_mapping('race'))).\
        append(_hispanic_create(df, covars)). \
        drop(['race', 'ethnicity'], 1).\
        sort_values(covars + ['race_ethnicity'])


def load_CBSA_cw():
    df_cw = pd.read_excel(
                'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls',
                header=2,
                skipfooter=4,
                usecols=[0, 3, 4, 9, 10],
                converters={
                    'FIPS State Code': lambda x: str(x) if len(str(x)) == 2 else f'0{x}',
                    'FIPS County Code': lambda x: str(x) if len(str(x)) == 3 else f'0{x}' if len(str(x)) == 2 else f'00{x}',
                }
            ).\
            rename(
                columns={
                    'FIPS State Code': 'fips_state',
                    'FIPS County Code': 'fips_county',
                    'Metropolitan/Micropolitan Statistical Area': 'area',
                    'CBSA Code': 'fips_msa'
                }
            ). \
            assign(fips_county = lambda x: x['fips_state'] + x['fips_county']).\
            astype({'fips_msa': 'str'})
    return df_cw