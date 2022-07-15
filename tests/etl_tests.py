import sys
import pandas as pd
import kauffman.constants as c
from kauffman.data import qwi
from kauffman.tools import mpj_indicators, county_msa_cross_walk, read_zip, state_msa_cross_walk, race_ethnicity_categories_create


def county_msa_cw():
    df = qwi(['Emp'], obs_level='county', state_list=['MO'], annualize=False).\
        pipe(county_msa_cross_walk, 'fips')
    print(df.head(10))


def state_msa_cw():
    df = state_msa_cross_walk(['29'])
    print(df)


def zip():
    url = 'https://www.federalreserve.gov/consumerscommunities/files/SHED_public_use_data_2018_(CSV).zip'
    df = read_zip(url, 'public2018.csv')
    print(df.head())


def mpj_heartland_state():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    df_temp = mpj_indicators(
        pd.read_csv(c.filenamer(f'../tests/data/qwi_heartland_industry.csv')),
        pd.read_csv(c.filenamer(f'../tests/data/pep_state.csv')),
        df_earnbeg_us,
        contribution_by=None,
        constancy_mult=100
    ).\
        astype({'industry': 'str'})
    df_temp['industry'] = df_temp['industry'].map(c.naics_code_to_abb(2))
    df_temp.to_csv(f'/Users/thowe/Downloads/mpj_heartland_industry.csv', index=False)


def mpj_heartland_metro_micro():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    df_temp = mpj_indicators(
        pd.read_csv(c.filenamer(f'../tests/data/qwi_heartland_firmage.csv')),
        pd.read_csv(c.filenamer(f'../tests/data/pep_msa.csv')),
        df_earnbeg_us,
        contribution_by=None,
        constancy_mult=100
    )
    df_temp['firmage'] = df_temp['firmage'].map(c.mpj_covar_mapping('firmage'))
    df_temp.to_csv(f'/Users/thowe/Downloads/mpj_heartland.csv', index=False)


def mpj_industry():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    for covar in ['race_hispanic']:
    # for covar in ['sex', 'agegrp', 'education', 'race_hispanic', 'industry']:
        df_temp = mpj_indicators(
            pd.read_csv(c.filenamer(f'../tests/data/qwi_us_{covar}_overall.csv')),
            # pd.read_csv(c.filenamer(f'../tests/data/qwi_us_{covar}.csv')),
            pd.read_csv(c.filenamer(f'../tests/data/pep_us.csv')),
            df_earnbeg_us,
            contribution_by=None,
            constancy_mult=100
        )
        print(df_temp.head(10))
        if covar not in ['race_hispanic', 'industry']:
            df_temp[covar] = df_temp[covar].map(c.mpj_covar_mapping(covar))
        elif covar == 'industry':
            df_temp[covar] = df_temp[covar].map(c.naics_code_to_abb(2))
        # elif covar == 'race_ethnicity':


        # df_temp.to_csv(f'/Users/thowe/Downloads/mpj_us_{covar}.csv', index=False)
        df_temp.to_csv(f'/Users/thowe/Downloads/mpj_us_{covar}_overall.csv', index=False)

    # df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    # mpj_indicators(
    #         pd.read_csv(c.filenamer(f'../tests/data/qwi_us_industry.csv')),
    #         pd.read_csv(c.filenamer(f'../tests/data/pep_us.csv')),
    #         df_earnbeg_us,
    #         contribution_by='firmage',
    #         constancy_mult=100
    #     ). \
    #     assign(firmage=lambda x: x['firmage'].map(c.mpj_covar_mapping('firmage'))).\
    #     assign(industry=lambda x: x['industry'].map(c.naics_code_to_abb(2))).\
    #     to_csv('/Users/thowe/Downloads/mpj_us_industry.csv', index=False)

    # df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    # pd.concat(
    #         [
    #             mpj_indicators(
    #                 pd.read_csv(c.filenamer(f'../tests/data/qwi_{region}.csv')),
    #                 pd.read_csv(c.filenamer(f'../tests/data/pep_{region}.csv')),
    #                 df_earnbeg_us
    #             )
    #             for region in ['us', 'state']
    #             # for region in ['us', 'state', 'msa', 'county']
    #         ]
    #     ).\
    #     to_csv(c.filenamer(f'../tests/data/mpj_industry.csv'), index=False)
    #

def mpj_covar():
    df_earnbeg_us = pd.read_csv(c.filenamer('../tests/data/earnbeg_us.csv'))
    for covar in ['race_ethnicity', 'sex', 'agegrp', 'education']:
        df_qwi = pd.read_csv(c.filenamer(f'../tests/data/qwi_us_{covar}.csv')).\
            assign(fips=0).\
            pipe(lambda x: race_ethnicity_categories_create(x, ['time', 'fips', 'region', 'ownercode', 'firmage']) if covar == 'race_ethnicity' else x)
        df_pep = pd.read_csv(c.filenamer(f'../tests/data/pep_us.csv'))

        df_temp = mpj_indicators(
            df_qwi,
            df_pep,
            df_earnbeg_us,
            contribution_by='firmage',
            constancy_mult=100
        )

        if covar != 'race_ethnicity':
            df_temp[covar] = df_temp[covar].map(c.mpj_covar_mapping(covar))
        df_temp['firmage'] = df_temp['firmage'].map(c.mpj_covar_mapping('firmage'))
        df_temp.to_csv(c.filenamer(f'/Users/thowe/downloads/mpj_{covar}.csv'), index=False)


def race_eth():
    df_temp = pd.read_csv(c.filenamer(f'../tests/data/qwi_us_race_ethnicity.csv')).assign(fips=0)
    covars = ['time', 'fips', 'region', 'ownercode', 'firmage']
    race_ethnicity_categories_create(df_temp, covars).\
        to_csv(c.filenamer(f'../tests/data/qwi_us_race_hispanic.csv'), index=False)

    df_temp = pd.read_csv(c.filenamer(f'../tests/data/qwi_us_race_ethnicity_overall.csv')).assign(fips=0)
    covars = ['time', 'fips', 'region', 'ownercode']
    race_ethnicity_categories_create(df_temp, covars).\
        to_csv(c.filenamer(f'../tests/data/qwi_us_race_hispanic_overall.csv'), index=False)


if __name__ == '__main__':
    mpj_heartland_metro_micro()
    # mpj_industry()
    # mpj_covar()

    # county_msa_cw()
    # state_msa_cw()

    # race_eth()
