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
    # pep(obs_level='us').\
    #     to_csv('/Users/thowe/Projects/indicator_shiny_app/pep_us.csv', index=False)
    pep(obs_level='msa').\
        to_csv(c.filenamer(f'../tests/data/pep_msa.csv'), index=False)

    # df = pep(obs_level='state')
    # df = pep(obs_level='msa')
    # df = pep(obs_level='county')


def qwi_test():
    # strata, msa
    df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', state_list=['MO'], private=True, annualize=True)
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

    # for covar in ['sex', 'agegrp', 'education']:
    #     qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmage'], worker_char=[covar], annualize=True).\
    #         to_csv(c.filenamer(f'../tests/data/qwi_us_{covar}.csv'), index=False)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['firmage'], worker_char=['race', 'ethnicity'], annualize=True).\
    #     to_csv(c.filenamer(f'../tests/data/qwi_us_race_ethnicity.csv'), index=False)


    # qwi by industry brief
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['NE', 'IA', 'MO', 'KS'], private=True, firm_char=['firmage', 'industry'], annualize=True).\
    qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='state', state_list=['NE', 'IA', 'MO', 'KS'], private=True, firm_char=['industry'], annualize=True).\
        query('industry != "92"').\
        to_csv(c.filenamer(f'../tests/data/qwi_heartland_industry.csv'), index=False)

    # for covar in ['sex', 'agegrp', 'education']:
    #     qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=[], worker_char=[covar], annualize=True).\
    #         to_csv(c.filenamer(f'../tests/data/qwi_us_{covar}_overall.csv'), index=False)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=[], worker_char=['race', 'ethnicity'], annualize=True).\
    #     to_csv(c.filenamer(f'../tests/data/qwi_us_race_ethnicity_overall.csv'), index=False)
    # qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='us', private=True, firm_char=['industry'], annualize=True).\
    #     to_csv(c.filenamer(f'../tests/data/qwi_us_industry_overall.csv'), index=False)


def qwi_msa_fetch():
    heartland_regions = ['28140', '41180', '36540', '19780', '48620', '44180', '27900', '26980', '16300', '19340', '30700', '45820', '27620',
                         '17860', '43580', '47940', '20220', '31740', '29940', '41460', '41140', '16020']

    qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', state_list=['NE', 'IA', 'MO', 'KS'], private=True, firm_char=['firmage'], annualize=True).\
        query(f'fips in {heartland_regions}'). \
        to_csv(c.filenamer(f'../tests/data/qwi_heartland_firmage.csv'), index=False)


def qwi_msa_fetch_fips():
    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', fips_list=['28140', '31740'], private=True, firm_char=['firmage'], annualize=True)
    # df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', state_list=['MO'], private=True, firm_char=['firmage'], annualize=True)
    df = qwi(['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC'], obs_level='msa', fips_list=['28140', '41180', '36540', '19780', '48620', '44180', '27900', '26980', '16300', '19340', '30700', '45820', '27620', '17860', '43580', '47940', '20220', '31740', '29940', '41460', '41140', '16020'], private=True, firm_char=['firmage'], annualize=True)
    print(df)


def _qwi_standardize_df(df):
    return df.\
        reset_index(drop=True). \
        sort_values(['fips', 'time', 'firmage'])


def qwi_compare_county_fetch():
    counties = [
        '01003', '01047', '01089', '06037', '06047', '06059', '06067', '17143', 
        '17131', '17113', '17089', '17077', '17043', '34007', '34001', '34033',
        '34027', '34015'
    ]
    indicators = ['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC']

    dfA = qwi(
            indicators, obs_level='county', private=True, firm_char=['firmage'], annualize=True,
            n_threads=50
        ). \
        query(f'fips in {counties}'). \
        pipe(_qwi_standardize_df)

    dfB = qwi(
            indicators, obs_level='county', state_list=['AL', 'CA', 'IL', 'NJ'], private=True,
            firm_char=['firmage'], annualize=True, n_threads=50
        ). \
        query(f'fips in {counties}'). \
        pipe(_qwi_standardize_df)

    dfC = qwi(
            indicators, obs_level='county', fips_list=counties, private=True, firm_char=['firmage'], annualize=True, n_threads=50
        ). \
        pipe(_qwi_standardize_df)

    if dfA.equals(dfB):
        if dfB.equals(dfC):
            return "Pass. dfA == dfB == dfC"
        else:
            return "Fail. dfA == B, but dfB != dfC"
    else:
        return "Fail. dfA != dfB"


def qwi_compare_msa_fetch():
    heartland_regions = [
        '28140', '41180', '36540', '19780', '48620', '44180', '27900', '26980',
        '16300', '19340', '30700', '45820', '27620', '17860', '43580', '47940',
        '20220', '31740', '29940', '41460', '41140', '16020'
    ]
    indicators = ['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC']

    dfA = qwi(
            indicators, obs_level='msa', private=True, firm_char=['firmage'], annualize=True,
            n_threads=50
        ). \
        query(f'fips in {heartland_regions}'). \
        pipe(_qwi_standardize_df)

    dfB = qwi(
            indicators, obs_level='msa', private=True, state_list=['NE', 'IA', 'MO', 'KS'],
            firm_char=['firmage'], annualize=True, n_threads=50
        ). \
        query(f'fips in {heartland_regions}'). \
        pipe(_qwi_standardize_df)
    
    dfC = qwi(
            indicators, obs_level='msa', fips_list=heartland_regions, private=True, 
            firm_char=['firmage'], annualize=True, n_threads=50
        ). \
        pipe(_qwi_standardize_df)

    if dfA.equals(dfB):
        if dfB.equals(dfC):
            return "Pass. dfA == dfB == dfC"
        else:
            return "Fail. dfA == dfB, but dfB != dfC"
    else:
        return "Fail. dfA != dfB"


def qwi_compare_state_fetch():
    states = ['IL', 'ND', 'AK', 'NC', 'RI', 'AZ', 'OR', 'MA', 'MD', 'CA', 'WY', 'MO']
    indicators = ['Emp', 'EmpEnd', 'EarnBeg', 'EmpS', 'EmpTotal', 'FrmJbC']

    dfA = qwi(
            indicators, obs_level='state', private=True, firm_char=['firmage'], annualize=True,
            n_threads=50
        ). \
        query(f'fips in {[c.state_abb_to_fips[s] for s in states]}'). \
        pipe(_qwi_standardize_df)

    dfB = qwi(
        indicators, obs_level='state', state_list=states, private=True, firm_char=['firmage'],
        annualize=True, n_threads=50
        ). \
        pipe(_qwi_standardize_df)


    if dfA.equals(dfB):
        return 'Pass. dfA == dfB'
    else:
        return 'Fail. dfA != dfB'



if __name__ == '__main__':
    # bfs_test()
    # qwi_test()
    # pep_test()

    # mpj_data_fetch()
    # qwi_msa_fetch()
    qwi_msa_fetch_fips()
