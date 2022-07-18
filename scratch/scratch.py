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
df = qwi(obs_level='county', firm_char=['firmage'], n_threads=30)
df = qwi(obs_level='msa', firm_char=['firmage'], n_threads=30)
df = qwi(obs_level='state', firm_char=['firmage'], n_threads=30)
df = qwi(obs_level='us', firm_char=['firmage'], n_threads=30)

# Playing with state_list
df = qwi(indicators, obs_level='state', state_list=['AL', 'HI'], n_threads=30)
df = qwi(indicators, obs_level='county', state_list=['CO'], n_threads=30)
df = qwi(indicators, obs_level='msa', state_list=['CO'], n_threads=30)
df = qwi(indicators, obs_level='msa', state_list=['KS'], n_threads=30) # Problem msas
df = qwi(
    indicators, obs_level='state', state_list=['CO', 'KY', 'HI', 'OR'],
    firm_char=['industry', 'firmage'], worker_char=['sex', 'agegrp'], n_threads=30
)
df = qwi(
    indicators, obs_level='state', state_list=['CT', 'DC'],
    firm_char=['firmage'], worker_char=['race', 'ethnicity'], n_threads=30
)
df = qwi(indicators, obs_level='state', worker_char=['education'], n_threads=30)
df = qwi(
    indicators, obs_level='msa', state_list=['GA', 'WY', 'TX', 'KS'],
    firm_char=['firmsize'], worker_char=['sex', 'education'], n_threads=30
)
df = qwi(
    indicators, obs_level='county', state_list=['AL', 'NY'],
    firm_char=['industry'], worker_char=['race'], n_threads=30
)


# Playing with missing counties + msas
df = qwi(indicators, obs_level='county', state_list=['AK', 'SD'], n_threads=30)
df = qwi(indicators, obs_level='msa', state_list=['KS', 'AL', 'WV'], n_threads=30)
df = qwi(indicators, obs_level='county', worker_char=['ethnicity'], n_threads=30)
df = qwi(indicators, obs_level='msa', n_threads=30)

# Fips_list
df = qwi(indicators, obs_level='county', fips_list=counties, n_threads=30)
df = qwi(indicators, obs_level='msa', fips_list=msas, n_threads=30)
df = qwi(indicators, obs_level='county', fips_list=counties, worker_char=['ethnicity'], n_threads=30)
df = qwi(indicators, obs_level='msa', fips_list=msas, firm_char=['industry'], n_threads=30)
df = qwi(
    indicators, obs_level='msa', fips_list=msas, firm_char=['industry'],
    worker_char=['education'], n_threads=30
)

# Private
df = qwi(indicators, obs_level='county', private=True, n_threads=30)
df = qwi(indicators, obs_level='msa', private=True, n_threads=30)
df = qwi(indicators, obs_level='state', private=True, n_threads=30)
df = qwi(indicators, obs_level='us', private=True, n_threads=30)

# Annualize
df = qwi(indicators, obs_level='county', worker_char=['sex'], annualize='January', n_threads=30)
df = qwi(indicators, obs_level='county', annualize='March', n_threads=30)
df = qwi(indicators, obs_level='county', annualize=False, n_threads=30)
df = qwi(indicators, obs_level='msa', annualize='January', n_threads=30)
df = qwi(indicators, obs_level='msa', firm_char=['firmsize'], annualize='March', n_threads=30)
df = qwi(indicators, obs_level='msa', annualize=False, n_threads=30)
df = qwi(indicators, obs_level='state', annualize='January', n_threads=30)
df = qwi(indicators, obs_level='state', annualize='March', n_threads=30)
df = qwi(indicators, obs_level='state', firm_char=['industry'], annualize=False, n_threads=30)

# Strata totals
df = qwi(indicators, obs_level='state', firm_char=['firmsize'], strata_totals=True, n_threads=30)
df = qwi(indicators, obs_level='state', worker_char=['sex'], strata_totals=True, n_threads=30)