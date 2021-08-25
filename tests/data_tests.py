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

def acs_test():
    df = acs(['B24092_004E', 'B24092_013E'])
    df = acs(series_lst=['B24081_001E'], obs_level='msa', state_lst='all')
    df = acs(series_lst='all', obs_level=['state', 'county'])
    df = acs(obs_level='county', state_lst=['CO'])
    df = acs(obs_level='state', state_lst=['CO', 'UT'])
    df = acs(obs_level='msa', state_lst=['HI', 'VT'])
    df = acs(obs_level='msa')
    df = acs(obs_level='us')


def bds_test():
    print(bds(['FIRM', 'FAGE'], obs_level='us', industry='11'))
    sys.exit()

    bds(['FIRM', 'FAGE'], obs_level='us', industry='all').\
        to_csv('/Users/thowe/Projects/data_science/mastering_shiny/indicator_app/bds_us.csv', index=False)


def bed_test():
    df = bed('firm size', 1)
    # df = bed('firm size', 2)
    # df = bed('firm size', 3)
    # df = bed('firm size', 4)
    # df = bed('1bf', obs_level=['AL', 'US', 'MO'])


def bfs_test():
    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['AZ'])
    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)

    bfs(['BA_BA'], obs_level='us', industry='all').\
        to_csv('/Users/thowe/Projects/data_science/mastering_shiny/indicator_app/bfs_us.csv', index=False)


def pep_test():
    pep(obs_level='us').\
        to_csv('/Users/thowe/Projects/indicator_shiny_app/pep_us.csv', index=False)

    # df = pep(obs_level='state')
    # df = pep(obs_level='msa')
    # df = pep(obs_level='county')


def qwi_test():
    # strata, msa
    df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', state_list=['06'], private=True, strata=['firmage'], annualize=True)
    print(df)
    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', state_list=['MO'], private=True, strata=['firmage'], annualize=True)
    # print(df.head(1000))

    # strata, state
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['MO'], private=True, strata=['firmsize', 'industry'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['MO'], private=True, strata=['firmage', 'industry'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['MO'], private=True, strata=['sex', 'agegrp'], annualize=True)

    # strata, us
    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=[], worker_char=['race'], annualize=True)
    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=[], worker_char=['education'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=False, firm_char=[], worker_char=['edgrp'], annualize=True)

    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmsize'], worker_char=['race'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmsize'], worker_char=['sex'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmsize'], worker_char=['agegrp'], annualize=True)
    qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmsize'], worker_char=['education'], annualize=True)

    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, strata=['firmage', 'industry'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, strata=['sex', 'agegrp'], annualize=True)


    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', private=True, strata=['firmage'], annualize=True)
    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, strata=['firmage'], annualize=True)
    # df = qwi(obs_level='county', state_list=['CO'])
    # df = qwi(obs_level='state', state_list=['UT'], strata=['firmsize'])
    # df = qwi(obs_level='msa', state_list=['CO', 'UT'], strata=['firmsize'])
    # df = qwi(obs_level='state')
    # df = qwi(obs_level='msa')
    # df = qwi(obs_level='county')

    # qwi(['Emp'], obs_level='us', strata=['firmage', 'industry']). \
    #     to_csv('/Users/thowe/Projects/indicator_shiny_app/qwi_us.csv', index=False)


def shed_test():
    df = shed()
    # df = shed('us', ['gender', 'race_ethnicity'], ['med_exp_12_months', 'man_financially'])
    # df = shed(['med_exp_12_months', 'man_financially'], 'us',)
    # print(df.head())


def mpj_data_fetch():
    ## setting private=False results in zero rows returned
    # qwi(['EarnBeg'], obs_level='us', private=True, annualize=True) \
    #     [['time', 'EarnBeg']]. \
    #     rename(columns={'EarnBeg': 'EarnBeg_us'}). \
    #     apply(pd.to_numeric). \
    #     to_csv(c.filenamer('../tests/data/earnbeg_us.csv'), index=False)

    # for region in ['us', 'state', 'msa', 'county']:
    # for region in ['us', 'state']:
    #     qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level=region, private=True, strata=['firmage', 'industry'], annualize=True).\
    #         to_csv(c.filenamer(f'../tests/data/qwi_{region}.csv'), index=False)
    #
    #     pep(region).\
    #         rename(columns={'POP': 'population'}). \
    #         to_csv(c.filenamer(f'../tests/data/pep_{region}.csv'), index=False)

    # todo: labels for the different categories. Do this in the etl part
    for covar in ['race_ethnicity', 'sex', 'agegrp', 'education']:
        qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmsize'], worker_char=[covar], annualize=True).\
            to_csv(c.filenamer(f'../tests/data/qwi_us_{covar}.csv'), index=False)


if __name__ == '__main__':
    # bfs_test()
    # qwi_test()
    mpj_data_fetch()
