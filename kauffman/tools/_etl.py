import io
import sys
import boto3
import requests
import numpy as np
import pandas as pd
from zipfile import ZipFile


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



def kese_indicators():
    pass
def neb_indicators():
    pass


def _mpj_raw_data_merge(df_qwi, df_pep, df_earnbeg_us):
    return df_qwi. \
        merge(df_pep[['fips', 'time', 'population']], how='left', on=['fips', 'time']).\
        merge(df_earnbeg_us, how='left', on='time')


def _missing_obs(df):
    df.loc[df['EmpTotal'] == 0, 'constancy'] = np.nan
    df.loc[df['EarnBeg'] == 0, 'compensation'] = np.nan
    df.loc[df['emp_mid'] == 0, 'contribution'] = np.nan
    return df


def mpj_indicators(df_qwi, df_pep, df_earnbeg_us):
    """
    todo: description of each of these data sets briefly
    """
    # todo: need some restrictions for variable names in each of the dfs
    #   each row needs to be unique category within fips/time

    # todo: I want to drop the variables from pep and earnbeg_us
    df_qwi.loc[(df_qwi['time'] == 1993) & (df_qwi['firmage'] == 1), 'Emp'] = np.nan
    df_qwi = df_qwi.loc[~((df_qwi['time'] == 1994) & (df_qwi['firmage'] == 1))]
    return _mpj_raw_data_merge(
            df_qwi,
            df_pep,
            df_earnbeg_us.rename(columns={'EarnBeg': 'EarnBeg_us'})
        ).\
        assign(
            emp_mid=lambda x: (x['Emp'] + x['EmpEnd']) / 2,
            within_count=lambda x: x[['emp_mid', 'fips', 'time']].groupby(['fips', 'time']).transform('count'),
            max_count=lambda x: x['within_count'].max(),
            total_emp=lambda x: x[['emp_mid', 'fips', 'time']].groupby(['fips', 'time']).transform(sum, min_count=int(x['max_count'].iloc[0]))
        ).\
        assign(
            contribution=lambda x: x['emp_mid'] / x['total_emp'],
            compensation=lambda x: x['EarnBeg'] / x['EarnBeg_us'],
            constancy=lambda x: (x['EmpS'] / x['EmpTotal']),
            creation=lambda x: x['FrmJbC'] / x['population'] * 1000,
        ).\
        pipe(_missing_obs).\
        drop(['emp_mid', 'within_count', 'max_count', 'total_emp'], 1)
