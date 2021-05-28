import boto3
import pandas as pd

def read_zip(zip_url, filename):
    """
    Reads a csv file from a zip file online. Example: 'public2018.csv' from
    'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip'
    """
    z = ZipFile(io.BytesIO(requests.get(zip_url).content))
    return pd.read_csv(z.open(filename), encoding='cp1252')


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


def county_msa_cross_walk(df_county, fips_county):
    """
    Receives county level data, and merges (on county fips) with a dataframe with CBSA codes.

    fips_county: fips column name
    """
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
        drop(['FIPS State Code', 'FIPS County Code'], 1)

    return df_county.\
        rename(columns={fips_county: 'fips_county'}).\
        merge(df_cw, how='left', on='fips_county')  #.\
        # query('fips == fips'). \
        # astype({'fips': 'int'})
#     todo: how far do I want to take this? I could do the entire MSA transformation, or just leave as the merged dataframe.



# 1. merge with cw, 2. groupby .