import boto3
import logging
import pandas as pd
from botocore.exceptions import ClientError


import os
import sys
import threading

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def aws_upload(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
        - e.g.: '/Users/thowe/Downloads/qwi_412506b63372496a91fad6c8029f9542.csv'
    :param bucket: Bucket to upload to
        - e.g.: 'emkf.data.research'
    :param object_name: S3 object name. If not specified then file_name is used. Subdirectories should precede the
        filename, separated by a slash.
        - e.g.: 'new_employer_businesses/qwi.csv
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(
            file_name, bucket, object_name,
            Callback=ProgressPercentage(file_name)
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def aws_download(file_name, bucket, object_name):
    s3 = boto3.client('s3')
    s3.download_file(
        bucket, object_name, file_name,
        Callback=ProgressPercentage(file_name)
    )


def _region_transform(df, v, df_out):
    covar_lst = [col for col in df.columns.tolist() if v + '_' in col and col.count('_') == 1]
    new_covar_lst = [col.replace('_', '') for col in covar_lst]
    df_v = df.\
        rename(columns={**dict(zip(covar_lst, new_covar_lst)), **{'sname': 'name', 'demtype': 'type', 'demographic': 'category'}}). \
        replace('Annual', 'Total').\
        assign(
            type=lambda x: 'Total' if 'type' not in x.columns else x['type'],
            category=lambda x: 'Total' if 'category' not in x.columns else x['category'],
            fips=lambda x: x['name'].map(c.us_state_abbrev).map(c.state_dic_temp)
        ) \
        [['name', 'fips', 'type', 'category'] + new_covar_lst].\
        query('category != "Ages 25-64"'). \
        pipe(pd.wide_to_long, v, i=['name', 'fips', 'type', 'category'], j='year'). \
        reset_index(). \
        assign(category=lambda x: pd.Categorical(x['category'], ['Total', 'Ages 20-34', 'Ages 35-44', 'Ages 45-54', 'Ages 55-64', 'Less than High School', 'High School Graduate', 'Some College', 'College Graduate', 'Native-Born', 'Immigrant', 'White', 'Black', 'Latino', 'Asian', 'Male', 'Female', 'Veterans', 'Non-Veterans']))

    if df_out.shape[1] == 0:
        return df_v
    return df_out.merge(df_v, how='left', on=['name', 'fips', 'type', 'category', 'year'])


def raw_kese_formatter(state_file_path, us_file_path):
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
        df_state = _region_transform(pd.read_excel(state_file_path, sheet_name=k), v, df_state)
        df_us = _region_transform(pd.read_excel(us_file_path, sheet_name=k), v, df_us)

    return df_state.\
        append(df_us).\
        sort_values(['fips', 'category']).\
        reset_index(drop=True)


def download_to_alley_formatter(df, covar_lst, outcome):
    """
    covar_lst: lst
        List of covariates that are used to stratify the data.
    outcome: str
        The column name of the outcome whose values become the cells of the dataframe.
    """
    # data_in = self._obj
    # self._validate_panel(data_in, covar_lst)
    # todo: include some validator here

    return df[['fips', 'year'] + covar_lst + [outcome]].\
        pipe(pd.pivot_table, index=['fips'] + covar_lst, columns='year', values=outcome).\
        reset_index().\
        replace('Total', '').\
        rename(columns={'type': 'demographic-type', 'category': 'demographic'})


if __name__ == '__main__':
    # aws_upload('/Users/thowe/Downloads/qwi_412506b63372496a91fad6c8029f9542.csv', 'emkf.data.research', 'new_employer_businesses/qwi.csv')
    aws_download('/Users/thowe/Downloads/qwi.csv', 'emkf.data.research', 'new_employer_businesses/qwi.csv')