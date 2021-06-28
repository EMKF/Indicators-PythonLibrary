import sys
import pandas as pd
import kauffman.constants as c
from kauffman.tools import mpj_indicators

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 40000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


def mpj_industry():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    pd.concat(
            [
                mpj_indicators(
                    pd.read_csv(c.filenamer(f'../tests/data/qwi_{region}.csv')),
                    pd.read_csv(c.filenamer(f'../tests/data/pep_{region}.csv')),
                    df_earnbeg_us
                )
                for region in ['us', 'state', 'msa', 'county']
            ]
        ).\
        to_csv(c.filenamer(f'../tests/data/mpj_industry.csv'), index=False)
# todo: finish finessing final dataset to look like mpj_download

if __name__ == '__main__':
    mpj_industry()
