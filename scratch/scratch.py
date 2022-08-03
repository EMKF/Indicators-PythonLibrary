from kauffman.data._qwi import qwi
import pandas as pd


indicators = ['Emp', 'EmpEnd', 'EmpS', 'HirAs', 'Sep', 'EarnBeg', 'FrmJbC']
counties = [
    '02013', '02063', '46102', '09001', '25001', '37001', '41067', '48043',
    '54049', '15009', '11001'
]
msas = [
    '47900', '18980', '31080', '36740', '19430', '14300', '40530', '34350',
    '39150', '23780', '38860'
]


# Calls in MPJ
df_1 = qwi(obs_level='county', firm_char=['firmage'], n_threads=30)
df_2 = qwi(obs_level='msa', firm_char=['firmage'], n_threads=30)
df_3 = qwi(obs_level='state', firm_char=['firmage'], n_threads=30)
df_4 = qwi(obs_level='us', firm_char=['firmage'], n_threads=30)

# Playing with state_list
df_5 = qwi(indicators, obs_level='state', state_list=['AL', 'HI'], n_threads=30)
df_6 = qwi(indicators, obs_level='county', state_list=['CO'], n_threads=30)
df_7 = qwi(indicators, obs_level='msa', state_list=['CO'], n_threads=30)
df_8 = qwi(indicators, obs_level='msa', state_list=['KS'], n_threads=30) # Problem msas
df_9 = qwi(
    indicators, obs_level='state', state_list=['CO', 'KY', 'HI', 'OR'],
    firm_char=['industry', 'firmage'], worker_char=['sex', 'agegrp'], n_threads=30
)
df_10 = qwi(
    indicators, obs_level='state', state_list=['CT', 'DC'],
    firm_char=['firmage'], worker_char=['race', 'ethnicity'], n_threads=30
)
df_11 = qwi(indicators, obs_level='state', worker_char=['education'], n_threads=30)
df_12 = qwi(
    indicators, obs_level='msa', state_list=['GA', 'WY', 'TX', 'KS'],
    firm_char=['firmsize'], worker_char=['sex', 'education'], n_threads=30
)
df_13 = qwi(
    indicators, obs_level='county', state_list=['AL', 'NY'],
    firm_char=['industry'], worker_char=['race'], n_threads=30
)

# Playing with missing counties + msas
df_14 = qwi(indicators, obs_level='county', state_list=['AK', 'SD'], n_threads=30)
df_15 = qwi(indicators, obs_level='msa', state_list=['KS', 'AL', 'WV'], n_threads=30)
df_16 = qwi(indicators, obs_level='county', worker_char=['ethnicity'], n_threads=30)
df_17 = qwi(indicators, obs_level='msa', n_threads=30)

# Fips_list
df_18 = qwi(indicators, obs_level='county', fips_list=counties, n_threads=30)
df_19 = qwi(indicators, obs_level='msa', fips_list=msas, n_threads=30)
df_20 = qwi(indicators, obs_level='county', fips_list=counties, worker_char=['ethnicity'], n_threads=30)
df_21 = qwi(indicators, obs_level='msa', fips_list=msas, firm_char=['industry'], n_threads=30)
df_22 = qwi(
    indicators, obs_level='msa', fips_list=msas, firm_char=['industry'],
    worker_char=['education'], n_threads=30
)

# Private
df_23 = qwi(indicators, obs_level='county', state_list=['VA', 'VT'], private=True, n_threads=30)
df_24 = qwi(indicators, obs_level='msa', state_list=['IA', 'IN'], private=True, n_threads=30)
df_25 = qwi(indicators, obs_level='state', private=True, n_threads=30)
df_26 = qwi(indicators, obs_level='state', worker_char=['agegrp'], private=True, n_threads=30)

# Annualize
df_27 = qwi(indicators, obs_level='county', annualize='March', n_threads=30)
df_28 = qwi(indicators, obs_level='county', annualize=False, n_threads=30)
df_29 = qwi(indicators, obs_level='msa', firm_char=['firmsize'], annualize='March', n_threads=30)
df_30 = qwi(indicators, obs_level='msa', annualize=False, n_threads=30)
df_31 = qwi(indicators, obs_level='state', annualize='March', n_threads=30)
df_32 = qwi(indicators, obs_level='state', firm_char=['industry'], annualize=False, n_threads=30)

# Strata totals
df_33 = qwi(indicators, obs_level='state', firm_char=['firmsize'], strata_totals=True, n_threads=30)
df_34 = qwi(indicators, obs_level='state', worker_char=['sex'], strata_totals=True, n_threads=30)






###############################################################################
# Calls in MPJ
df_1_old = qwi(obs_level='county', firm_char=['firmage'], n_threads=30)
df_2_old = qwi(obs_level='msa', firm_char=['firmage'], n_threads=30)
df_3_old = qwi(obs_level='state', firm_char=['firmage'], n_threads=30)
df_4_old = qwi(obs_level='us', firm_char=['firmage'], n_threads=30)

# Playing with state_list
df_5_old = qwi(indicators, obs_level='state', state_list=['AL', 'HI'], n_threads=30)
df_6_old = qwi(indicators, obs_level='county', state_list=['CO'], n_threads=30)
df_7_old = qwi(indicators, obs_level='msa', state_list=['CO'], n_threads=30)
df_8_old = qwi(indicators, obs_level='msa', state_list=['KS'], n_threads=30) # Problem msas
df_9_old = qwi(
    indicators, obs_level='state', state_list=['CO', 'KY', 'HI', 'OR'],
    firm_char=['industry', 'firmage'], worker_char=['sex', 'agegrp'], n_threads=30
)
df_10_old = qwi(
    indicators, obs_level='state', state_list=['CT', 'DC'],
    firm_char=['firmage'], worker_char=['race', 'ethnicity'], n_threads=30
)
df_11_old = qwi(indicators, obs_level='state', worker_char=['education'], n_threads=30)
df_12_old = qwi(
    indicators, obs_level='msa', state_list=['GA', 'WY', 'TX', 'KS'],
    firm_char=['firmsize'], worker_char=['sex', 'education'], n_threads=30
)
df_13_old = qwi(
    indicators, obs_level='county', state_list=['AL', 'NY'],
    firm_char=['industry'], worker_char=['race'], n_threads=30
)

# Playing with missing counties + msas
df_14_old = qwi(indicators, obs_level='county', state_list=['AK', 'SD'], n_threads=30)
df_15_old = qwi(indicators, obs_level='msa', state_list=['KS', 'AL', 'WV'], n_threads=30)
df_16_old = qwi(indicators, obs_level='county', worker_char=['ethnicity'], n_threads=30)
df_17_old = qwi(indicators, obs_level='msa', n_threads=30)

# Fips_list
df_18_old = qwi(indicators, obs_level='county', fips_list=counties, n_threads=30)
df_19_old = qwi(indicators, obs_level='msa', fips_list=msas, n_threads=30)
df_20_old = qwi(indicators, obs_level='county', fips_list=counties, worker_char=['ethnicity'], n_threads=30)
df_21_old = qwi(indicators, obs_level='msa', fips_list=msas, firm_char=['industry'], n_threads=30)
df_22_old = qwi(
    indicators, obs_level='msa', fips_list=msas, firm_char=['industry'],
    worker_char=['education'], n_threads=30
)

# Private
df_23_old = qwi(indicators, obs_level='county', state_list=['VA', 'VT'], private=True, n_threads=30)
df_24_old = qwi(indicators, obs_level='msa', state_list=['IA', 'IN'], private=True, n_threads=30)
df_25_old = qwi(indicators, obs_level='state', private=True, n_threads=30)
df_26_old = qwi(indicators, obs_level='state', worker_char=['agegrp'], private=True, n_threads=30)

# Annualize
df_27_old = qwi(indicators, obs_level='county', annualize='March', n_threads=30)
df_28_old = qwi(indicators, obs_level='county', annualize=False, n_threads=30)
df_29_old = qwi(indicators, obs_level='msa', firm_char=['firmsize'], annualize='March', n_threads=30)
df_30_old = qwi(indicators, obs_level='msa', annualize=False, n_threads=30)
df_31_old = qwi(indicators, obs_level='state', annualize='March', n_threads=30)
df_32_old = qwi(indicators, obs_level='state', firm_char=['industry'], annualize=False, n_threads=30)

# Strata totals
df_33_old = qwi(indicators, obs_level='state', firm_char=['firmsize'], strata_totals=True, n_threads=30)
df_34_old = qwi(indicators, obs_level='state', worker_char=['sex'], strata_totals=True, n_threads=30)


for i in range(7,13):
    result = eval(f'df_{i}').equals(eval(f'df_{i}_old'))
    print(i, result)