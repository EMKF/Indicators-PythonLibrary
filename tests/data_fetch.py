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


def mpj_data_fetch():
    qwi(['EarnBeg'], obs_level='us', private=True, annualize=True) \
        [['time', 'EarnBeg']]. \
        rename(columns={'EarnBeg': 'EarnBeg_us'}). \
        apply(pd.to_numeric). \
        to_csv(c.filenamer('../tests/data/earnbeg_us.csv'), index=False)

    for region in ['us', 'state', 'msa', 'county']:
        qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level=region, private=True, strata=['industry'], annualize=True).\
            to_csv(c.filenamer(f'../tests/data/qwi_{region}.csv'), index=False)

        pep(region).\
            rename(columns={'POP': 'population'}). \
            to_csv(c.filenamer(f'../tests/data/pep_{region}.csv'), index=False)


if __name__ == '__main__':
    mpj_data_fetch()
