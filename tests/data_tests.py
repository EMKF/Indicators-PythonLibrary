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


def bds_test():
    df = bds(['FIRM', 'FAGE', 'ESTAB', 'EAGE'], obs_level='us').\
        query('time == 2018').\
        query('FAGE not in [1, 150]')
    print(df)

    df[['FIRM', 'FAGE']].plot.bar(x='FAGE', y='FIRM')
    import matplotlib.pyplot as plt
    plt.show()


def bed_test():
    df = bed('firm size', 1)
    # df = bed('firm size', 2)
    # df = bed('firm size', 3)
    # df = bed('firm size', 4)
    # df = bed('1bf', obs_level=['AL', 'US', 'MO'])


def bfs_test():
    df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level=['AZ'])
    # df = bfs(['BA_BA', 'BF_SBF8Q'], obs_level='state')
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['AZ'], annualize=True)
    # df = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level=['US', 'AK'], march_shift=True)


def pep_test():
    df = pep(obs_level='us')
    # df = pep(obs_level='state')
    # df = pep(obs_level='msa')
    # df = pep(obs_level='county')


def qwi_test():
    # strata, state
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['MO'], private=True, strata=['firmsize', 'industry'], annualize=True)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['MO'], private=True, strata=['firmage', 'industry'], annualize=True)
    qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['MO'], private=True, strata=['sex', 'agegrp'], annualize=True)

    # strata, us
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, strata=['firmsize', 'industry'], annualize=True)
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
    for region in ['us', 'state']:
        qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level=region, private=True, strata=['firmage', 'industry'], annualize=True).\
            to_csv(c.filenamer(f'../tests/data/qwi_{region}.csv'), index=False)

        pep(region).\
            rename(columns={'POP': 'population'}). \
            to_csv(c.filenamer(f'../tests/data/pep_{region}.csv'), index=False)


if __name__ == '__main__':
    # qwi_test()

    # mpj_data_fetch()

    bds_test()