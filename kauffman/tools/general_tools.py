import boto3
import pandas as pd
from kauffman import constants as c


def file_to_s3(file, s3_bucket, s3_file):
    """
    Upload a local file to S3.

    file: str
        location of the local file
    s3_bucket: str
        Name of the S3 bucket. E.g., 'emkf.data.research'
    s3_file: str
        Destination file location in S3. 
        E.g., 'indicators/nej/data_outputs/your_pickle_filename.pkl'

    Examples:
        from kauffman.data import file_to_s3
        file_to_s3('local_file_path.csv', 'emkf.data.research', 
        'indicators/nej/data_outputs/remote_file_path.csv')
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
        File location in S3. 
        E.g., 'indicators/nej/data_outputs/your_pickle_filename.pkl'

    Examples:
        from kauffman.data import file_from_s3
        file_from_s3('local_file_path.csv', 'emkf.data.research', 
        'indicators/nej/data_outputs/remote_file_path.csv')
    """
    s3 = boto3.client('s3')
    s3.download_fileobj(bucket, key, file)


def CBSA_crosswalk():
    """Load and clean the Census's CBSA to FIPS County crosswalk (2020)"""

    url = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls'
    df_cw = pd.read_excel(
            url,
            header=2,
            skipfooter=4,
            usecols=[0, 3, 4, 9, 10],
            converters={
                'FIPS State Code': lambda x: str(x) if len(str(x)) == 2 \
                    else f'0{x}',
                'FIPS County Code': lambda x: str(x) if len(str(x)) == 3 \
                    else f'0{x}' if len(str(x)) == 2 \
                    else f'00{x}',
            }
        ) \
        .assign(
            fips_county=lambda x: x['FIPS State Code'] + x['FIPS County Code']
        ) \
        .rename(
            columns={
                'FIPS State Code': 'fips_state',
                'Metropolitan/Micropolitan Statistical Area': 'area',
                'CBSA Code': 'fips_msa'
            }
        ) \
        .astype({'fips_msa': 'str'}) \
        .append(
            pd.DataFrame(
                [
                    ['27980', 'Kahului-Wailuku-Lahaina, HI', 
                        'Metropolitan Statistical Area', '15', '005', '15005'],
                    ['31340', 'Lynchburg, VA', 'Metropolitan Statistical Area',
                        '51', '515', '51515']
                ],
                columns=[
                    'fips_msa', 'CBSA Title', 'area', 'fips_state', 
                    'FIPS County Code', 'fips_county'
                ]
            )
        ) \
        .reset_index(drop=True)

    return df_cw


def aggregate_county_to_msa(
    df_county, county_fips_colname, outcomes, agg_method=sum
):
    """
    Receives county level data, merges (on county fips) with a dataframe 
    with CBSA codes, and aggregates to the MSA level.

    county_fips_colname: fips column name

    Parameters
    ----------
    df_county: DataFrame
        The county-level data to aggregate
    county_fips_colname: str
        The name of the column with county fips codes
    outcomes: list or str
        The outcome or list of outcomes to aggregate
    agg_method: str or function, by default sum
        The method of aggregation

    Returns
    -------
    DataFrame
        MSA level data
    """
    
    outcomes = [outcomes] if type(outcomes) == str else outcomes
    df_county[outcomes] = df_county[outcomes].apply(pd.to_numeric)

    df_cw = CBSA_crosswalk()

    return df_county \
        .rename(columns={county_fips_colname: 'fips_county'}) \
        .merge(df_cw, how='left', on='fips_county') \
        [['fips_msa', 'CBSA Title', 'time'] + outcomes] \
        .groupby(['fips_msa', 'CBSA Title', 'time']).agg(agg_method) \
        .reset_index(drop=False) \
        .rename(columns={'CBSA Title': 'region', 'fips_msa':'fips'})
# todo: I can't just groupby and sum wrt cw(), since there might be missing 
# county values


def geolevel_crosswalk(
    from_geo, to_geo, from_fips_list, msa_coidentify_state=False
):
    """
    A crosswalk from one geographic level to another, or to multiple others.

    Parameters
    ----------
    from_geo: str ('state', 'msa', or 'county')
        The geographic level to map from
    to_geo: str, list ('state', 'msa', 'county', or combination)
        The geographic level(s) to map to
    from_fips_list: list
        The list of fips codes to map from
    msa_coidentify_state: bool, optional, by default False
        Whether to identify MSAs by both the MSA code and the state code, as 
        opposed to just the MSA code--ie: If MSA "M" goes across states "s1" 
        and "s2", msa_coidentify_state=True would result in that msa taking up 
        two rows (identified by fips_msa = "M" + fips_state = "s1", and fips_msa
        = "M" + fips_state = "s2"), whereas msa_coidentify_state=False would
        cause it to take up one row (identified by fips_msa = "M").

    Returns
    -------
    DataFrame
        Crosswalk between geographic levels
    """
    to_geo = [to_geo] if type(to_geo) == str else to_geo
    if msa_coidentify_state and ('msa' in to_geo or from_geo == 'msa'):
        to_geo = to_geo + ['state'] if 'state' not in to_geo else to_geo

    cw = CBSA_crosswalk()

    if (
        msa_coidentify_state 
        and from_geo == 'state' 
        and set(to_geo) == {'msa', 'state'}
    ):
        cw = cw \
            .query(f'fips_state in {from_fips_list}') \
            .drop_duplicates('fips_msa') \
            [['fips_msa']] \
            .merge(cw, how='left', on='fips_msa') \
            [['fips_state', 'fips_msa']]
    else:
        cw = cw[
                [f'fips_{from_geo}'] \
                + [f'fips_{geo}' for geo in to_geo if geo != from_geo]
            ] \
            .query(f'fips_{from_geo} in {from_fips_list}')

    return cw \
        .drop_duplicates() \
        .reset_index(drop=True)


def weighted_sum(df, strata=[], var_list='all', weight_var=None):
    """
    Performs weighted sum on the data.

    Parameters
    ----------
    df: DataFrame
        Input data
    strata: list, optional
        The list of variables that the data is stratified by, or the index.
    var_list: list or 'all', by default 'all'
        _description_
    weight_var: str, optional
        The weight variable.

    Returns
    -------
    DataFrame
        Aggregated data
    """
    strata = [strata] if type(strata) == str else strata
    if var_list == 'all':
        var_list = [x for x in df.columns if x not in strata]
    if weight_var == None:
        weight_var = 'weight'
        df['weight'] = 1
    
    return df[strata + var_list] \
        .apply(lambda x: x*df[weight_var] if x.name in var_list else x) \
        .groupby(strata).sum() \
        .reset_index()


def as_list(object):
    """Return list of passed object, unless already a list"""
    if type(object) == list:
        return object
    else:
        return [object]


def naics_code_key(digits, pub_admin=False):
    """
    Creates key of NAICS codes to their names

    Parameters
    ----------
    digits: int
        The level of industry to include (ie: 2-digit NAICS, 3-digit, etc.)
    pub_admin: bool, by default False
        Whether to include Public Administration in the key

    Returns
    -------
    DataFrame
        The key mapping NAICS codes to their names
    """
    return pd.read_csv('https://www2.census.gov/programs-surveys/bds/technical-documentation/label_naics.csv') \
        .query(f'indlevel == {digits}') \
        .drop(columns='indlevel') \
        .query(
            'name not in ["Public Administration", "Unclassified"]'
            if not pub_admin else ''
        )