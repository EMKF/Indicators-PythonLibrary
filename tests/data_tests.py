import kauffman.constants as c
from kauffman.data import acs, bfs, bds, pep, bed, qwi, shed


############### ACS tests ###################
# Series_lst examples
acs1 = acs(series_lst=['B24092_004E', 'B24092_013E'])
acs2 = acs(series_lst=['B24081_001E'], obs_level='state', state_lst='all')
acs3 = acs(series_lst='all', obs_level='us')

# State list examples
acs4 = acs(obs_level='county', state_lst=['WY'])
acs5 = acs(obs_level='msa', state_lst=['HI', 'VT'])
acs6 = acs(obs_level='msa', state_lst='all')
acs7 = acs(obs_level='state', state_lst=['CO', 'UT'])
acs8 = acs(obs_level='state', state_lst='all')



############### BED tests ###################
# KESE usage
bed1 = bed('1bf', obs_level=['AL', 'US', 'MO'])
bed2 = bed(series='establishment age and survival', table='1bf', obs_level='us')
bed3 = bed(series='establishment age and survival', table='1bf', obs_level='state')
bed4 = bed(series='establishment age and survival', table=7, obs_level='us')
bed5 = bed(series='establishment age and survival', table=7, obs_level='state')

# Firm size series examples
bed6 = bed(series='firm size', table=1)
bed7 = bed(series='firm size', table=2, obs_level='state')
bed8 = bed(series='firm size', table=3, obs_level='us')
bed9 = bed(series='firm size', table=4)

# Industry examples
bed10 = bed(series='firm size', table=5, naics='54')
bed11 = bed(series='firm size', table=6 , naics='81')


############### BDS tests ###################
# NEB usage
bds1 = bds(['FIRM'], obs_level='us')
bds2 = bds(['FIRM'], obs_level='state')

# Strata examples
bds3 = bds(series_lst=['ESTAB'], obs_level='us', strata=[])
bds4 = bds(series_lst=['FIRM', 'DENOM'], obs_level='us', strata=['NAICS'])
bds5 = bds(series_lst=['JOB_CREATION', 'FIRMDEATH_EMP'], obs_level='us', strata=['FAGE', 'GEOCOMP', 'METRO'])
bds6 = bds(series_lst=['EMP'], obs_level='state', strata=['FAGE'])
bds7 = bds(series_lst=['ESTABS_ENTRY'], obs_level='state', strata=['GEOCOMP', 'METRO'])
bds8 = bds(series_lst=['JOB_DESTRUCTION_DEATHS'], obs_level='msa', strata = ['EMPSZFI', 'NAICS'])
bds9 = bds(series_lst=['JOB_CREATION_BIRTHS', 'JOB_CREATION_CONTINUERS'], obs_level='msa', strata = ['EMPSZFII'])
bds10 = bds(series_lst=['REALLOCATION_RATE'], obs_level='county', strata = ['EMPSZFI'])



############### BFS tests ###################
# NEB Usage
bfs1 = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], region='us', annualize=True)
bfs2 = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], region='state', annualize=True)
bfs3 = bfs(['BF_SBF8Q'], region='us', march_shift=True)
bfs4 = bfs(['BF_SBF8Q'], region='state', march_shift=True)

# Industry examples
bfs5 = bfs(['BF_DUR4Q', 'BF_SBF8Q'], obs_level='state', industry='11')
bfs6 = bfs(['BF_SBF4Q', 'BF_PBF8Q'], obs_level='us', industry='44-45')
bfs7 = bfs(['BA_BA'], obs_level='us', industry='all')

# Seasonally_adj examples
bfs8 = bfs(['BF_PBF4Q', 'BF_BF8Q'], region='us', seasonally_adj=False)
bfs9 = bfs(['BA_WBA', 'BA_HBA'], region='state', seasonally_adj=False)
bfs10 = bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], region='us', seasonally_adj=True)

# Annualize examples
bfs11 = bfs(['BF_BF4Q', 'BA_CBA'], obs_level='state', annualize=True)
bfs12 = bfs(['BF_SBF8Q', 'BA_WBA'], obs_level='us', annualize=True)
bfs13 = bfs(['BF_DUR4Q', 'BA_WBA'], obs_level='us', annualize=False)

# March_shift examples
bfs14 = bfs(['BA_BA'], obs_level='us', march_shift=True)
bfs15 = bfs(['BA_HBA'], obs_level='state', march_shift=True)
bfs16 = bfs(['BF_DUR4Q'], obs_level='state', march_shift=False)

# Combinations
bfs14 = bfs(['BA_BA'], obs_level='us', industry='48-49', seasonally_adj=False, annualize=True, march_shift=True)
bfs15 = bfs(['BA_BA'], obs_level='state', industry='62', annualize=True, march_shift=True)



############### PEP tests ###################
pep1 = pep(obs_level='us')
pep2 = pep(obs_level='msa')
pep3 = pep(obs_level='county')
pep4 = pep(obs_level='state')


############### QWI tests ###################
indicators = ['Emp', 'EmpEnd', 'EmpS', 'HirAs', 'Sep', 'EarnBeg', 'FrmJbC']
counties = [
    '02013', '02063', '46102', '09001', '25001', '37001', '41067', '48043',
    '54049', '15009', '11001'
]
msas = [
    '47900', '18980', '31080', '36740', '19430', '14300', '40530', '34350',
    '39150', '23780', '38860'    
]

# MPJ Usage
qwi1 = qwi(obs_level='county', firm_char=['firmage'], n_threads=30)
qwi2 = qwi(obs_level='msa', firm_char=['firmage'], n_threads=30)
qwi3 = qwi(obs_level='state', firm_char=['firmage'], n_threads=30)
qwi4 = qwi(obs_level='us', firm_char=['firmage'], n_threads=30)
qwi5 = qwi(['EarnBeg'], obs_level='us', private=True, annualize=True)

# State_list examples
qwi6 = qwi(indicator_list=indicators obs_level='state', state_list=['AL', 'HI'], n_threads=30)
qwi7 = qwi(indicator_list=indicators obs_level='county', state_list=['CO'], n_threads=30)
qwi8 = qwi(indicator_list=indicators obs_level='msa', state_list=['CO'], n_threads=30)
qwi9 = qwi(indicator_list=indicators obs_level='msa', state_list=['KS'], n_threads=30) # Problem msas
qwi10 = qwi(indicator_list=indicators obs_level='state', state_list=['CO', 'KY', 'HI', 'OR'], firm_char=['industry', 'firmage'], worker_char=['sex', 'agegrp'], n_threads=30)
qwi11 = qwi(indicator_list=indicators obs_level='state', state_list=['CT', 'DC'], firm_char=['firmage'], worker_char=['race', 'ethnicity'], n_threads=30)
qwi12 = qwi(indicator_list=indicators obs_level='state', worker_char=['education'], n_threads=30)
qwi13 = qwi(indicator_list=indicators obs_level='msa', state_list=['GA', 'WY', 'TX', 'KS'], firm_char=['firmsize'], worker_char=['sex', 'education'], n_threads=30)
qwi14 = qwi(indicator_list=indicators obs_level='county', state_list=['AL', 'NY'], firm_char=['industry'], worker_char=['race'], n_threads=30)

# Call with missing counties/msa's
qwi15 = qwi(indicator_list=indicators obs_level='county', state_list=['AK', 'SD'], n_threads=30)
qwi16 = qwi(indicator_list=indicators obs_level='msa', state_list=['KS', 'AL', 'WV'], n_threads=30)
qwi17 = qwi(indicator_list=indicators obs_level='county', worker_char=['ethnicity'], n_threads=30)
qwi18 = qwi(indicator_list=indicators obs_level='msa', n_threads=30)

# Fips list examples
qwi19 = qwi(indicator_list=indicators obs_level='county', fips_list=counties, n_threads=30)
qwi20 = qwi(indicator_list=indicators obs_level='msa', fips_list=msas, n_threads=30)
qwi21 = qwi(indicator_list=indicators obs_level='county', fips_list=counties, worker_char=['ethnicity'], n_threads=30)
qwi22 = qwi(indicator_list=indicators obs_level='msa', fips_list=msas, firm_char=['industry'], n_threads=30)
qwi23 = qwi(indicator_list=indicators obs_level='msa', fips_list=msas, firm_char=['industry'], worker_char=['education'], n_threads=30)

# Private examples
qwi24 = qwi(indicator_list=indicators obs_level='county', state_list=['VA', 'VT'], private=True, n_threads=30)
qwi25 = qwi(indicator_list=indicators obs_level='msa', state_list=['IA', 'IN'], private=True, n_threads=30)
qwi26 = qwi(indicator_list=indicators obs_level='state', private=True, n_threads=30)
qwi27 = qwi(indicator_list=indicators obs_level='state', worker_char=['agegrp'], private=True, n_threads=30)

# Annualize examples
qwi28 = qwi(indicator_list=indicators obs_level='county', annualize='March', n_threads=30)
qwi29 = qwi(indicator_list=indicators obs_level='county', annualize=False, n_threads=30)
qwi30 = qwi(indicator_list=indicators obs_level='msa', firm_char=['firmsize'], annualize='March', n_threads=30)
qwi31 = qwi(indicator_list=indicators obs_level='msa', annualize=False, n_threads=30)
qwi32 = qwi(indicator_list=indicators obs_level='state', annualize='March', n_threads=30)
qwi33 = qwi(indicator_list=indicators obs_level='state', firm_char=['industry'], annualize=False, n_threads=30)

# Strata totals examples
qwi34 = qwi(indicator_list=indicators obs_level='state', firm_char=['firmsize'], strata_totals=True, n_threads=30)
qwi35 = qwi(indicator_list=indicators obs_level='state', worker_char=['sex'], strata_totals=True, n_threads=30)


############### SHED tests ###################
shed1 = shed('us', ['gender', 'race_ethnicity'], ['med_exp_12_months', 'man_financially'])
shed2 = shed(['med_exp_12_months', 'man_financially'], 'us')