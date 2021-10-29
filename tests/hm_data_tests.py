import sys
import pandas as pd
import kauffman.constants as c
from kauffman.data import acs, bfs, bds, pep, bed, qwi, shed

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 40000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def bfs_tester_hm():
    df = bfs(['BA_BA'], \
             obs_level='all', industry='00', annualize=True)
    print(df.head())



if __name__ == '__main__':
    bfs_tester_hm()