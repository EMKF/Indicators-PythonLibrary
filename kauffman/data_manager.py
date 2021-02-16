import io
import zipfile
import requests
import pandas as pd

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

def zip_to_dataframe(url):
    """
    Takes a url and reads in the .dat file. Assumes only one file inside the zip and that it is .dat formatted.
used in ase module
    TODO: generalize this.
    """
    r = requests.get(url)
    filebytes = io.BytesIO(r.content)
    myzipfile = zipfile.ZipFile(filebytes)
    for name in myzipfile.namelist():
        return pd.read_csv(myzipfile.open(name), sep='|', low_memory=False)


