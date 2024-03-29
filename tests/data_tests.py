import kauffman.constants as c
from kauffman.data import acs, bfs, bds, pep, bed, qwi
from datetime import datetime as dt


############### ACS tests ###################
# Series_lst examples
acs1 = "acs(series_lst=['B24092_004E', 'B24092_013E'], obs_level='state')"
acs2 = "acs(series_lst=['B24081_001E'], obs_level='state', state_lst='all')"
acs3 = "acs(series_lst='all', obs_level='us')"

# State list examples
acs4 = "acs(obs_level='county', state_lst=['WY'])"
acs5 = "acs(obs_level='msa', state_lst=['HI', 'VT'])"
acs6 = "acs(obs_level='msa', state_lst='all')"
acs7 = "acs(obs_level='state', state_lst=['CO', 'UT'])"
acs8 = "acs(obs_level='state', state_lst='all')"


############### BED tests ###################
# KESE usage
bed1 = "bed(series='establishment age and survival', table='1bf', obs_level='us')"
bed2 = "bed(series='establishment age and survival', table='1bf', obs_level='state')"
bed3 = "bed(series='establishment age and survival', table=7, obs_level='us')"
bed4 = "bed(series='establishment age and survival', table=7, obs_level='state')"

# Firm size series examples
bed5 = "bed(series='firm size', table=1)"
bed6 = "bed(series='firm size', table=2)"
bed7 = "bed(series='firm size', table=3)"
bed8 = "bed(series='firm size', table=4)"

# Industry examples
bed9 = "bed(series='establishment age and survival', obs_level='us', table=5, industry='54')"
bed10 = "bed(series='establishment age and survival', obs_level='state', table=2, industry='81')"


############### BDS tests ###################
# NEB usage
bds1 = "bds(['FIRM'], obs_level='us')"
bds2 = "bds(['FIRM'], obs_level='state')"

# Strata examples
bds3 = "bds(series_lst='all', obs_level='us', strata=[])"
bds4 = "bds(series_lst=['FIRM', 'DENOM'], obs_level='us', strata=['NAICS'])"
bds5 = "bds(series_lst=['JOB_CREATION', 'FIRMDEATH_EMP'], obs_level='us', strata=['FAGE', 'GEOCOMP', 'METRO'])"
bds6 = "bds(series_lst='all', obs_level='state', strata=['FAGE'])"
bds7 = "bds(series_lst=['ESTABS_ENTRY'], obs_level='state', strata=['GEOCOMP', 'METRO'])"
bds8 = "bds(series_lst='all', obs_level='msa', strata = ['EMPSZFI'])"
bds9 = "bds(series_lst=['JOB_CREATION_BIRTHS', 'JOB_CREATION_CONTINUERS'], obs_level='msa', strata = ['EMPSZFII'])"
bds10 = "bds(series_lst=['REALLOCATION_RATE'], obs_level='county', strata = ['FAGE'])"

# State_list examples
bds11 = "bds(series_lst='all', obs_level='state', state_list=['PA', 'TN'])"
bds12 = "bds(series_lst='all', obs_level='county', state_list=['ND'])"
bds13 = "bds(series_lst=['FIRM'], obs_level='state', state_list=['NJ', 'NY', 'MI'], strata=['EAGE', 'NAICS'])"


############### BFS tests ###################
# NEB Usage
bfs1 = "bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level='us', annualize=True)"
bfs2 = "bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level='state', annualize=True)"
bfs3 = "bfs(['BF_SBF8Q'], obs_level='us', march_shift=True)"
bfs4 = "bfs(['BF_SBF8Q'], obs_level='state', march_shift=True)"

# Industry examples
bfs5 = "bfs(['BF_DUR4Q', 'BF_SBF8Q'], obs_level='state', industry='11')"
bfs6 = "bfs(['BF_SBF4Q', 'BF_PBF8Q'], obs_level='us', industry='44-45')"
bfs7 = "bfs(['BA_BA'], obs_level='us', industry='all')"

# Seasonally_adj examples
bfs8 = "bfs('all', obs_level='us', seasonally_adj=False)"
bfs9 = "bfs(['BA_WBA', 'BA_HBA'], obs_level='state', seasonally_adj=False)"
bfs10 = "bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], obs_level='us', seasonally_adj=True)"

# Annualize examples
bfs11 = "bfs(obs_level='state', annualize=True)"
bfs12 = "bfs(['BF_SBF8Q', 'BA_WBA'], obs_level='us', annualize=True)"
bfs13 = "bfs(['BF_DUR4Q', 'BA_WBA'], obs_level='us', annualize=False)"

# March_shift examples
bfs14 = "bfs(['BA_BA'], obs_level='us', march_shift=True)"
bfs15 = "bfs(['BA_HBA'], obs_level='state', march_shift=True)"
bfs16 = "bfs(['BF_DUR4Q'], obs_level='state', march_shift=False)"

# Combinations
bfs17 = "bfs('all', obs_level='us', industry='48-49', seasonally_adj=False, annualize=True, march_shift=True)"
bfs18 = "bfs(['BA_BA'], obs_level='state', annualize=True, march_shift=True)"


############### PEP tests ###################
pep1 = "pep(obs_level='us')"
pep2 = "pep(obs_level='msa')"
pep3 = "pep(obs_level='county')"
pep4 = "pep(obs_level='state')"

# State_list examples
pep5 = "pep(obs_level='msa', state_list=['CO', 'UT'])"
pep6 = "pep(obs_level='county', state_list=['GA', 'HI'])"
pep7 = "pep(obs_level='state', state_list=['IN', 'AL', 'AK', 'TX'])"


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

# EJI Usage
qwi1 = "qwi(obs_level='county', firm_char=['firmage'], n_threads=30)"
qwi2 = "qwi(obs_level='msa', firm_char=['firmage'], n_threads=30)"
qwi3 = "qwi(obs_level='state', firm_char=['firmage'], n_threads=30)"
qwi4 = "qwi(obs_level='us', firm_char=['firmage'], n_threads=30)"
qwi5 = "qwi(['EarnBeg'], obs_level='us', private=True, annualize=True)"

# State_list examples
qwi6 = "qwi(indicator_list=indicators, obs_level='state', state_list=['AL', 'HI'], n_threads=30)"
qwi7 = "qwi(indicator_list=indicators, obs_level='county', state_list=['CO'], n_threads=30)"
qwi8 = "qwi(indicator_list=indicators, obs_level='msa', state_list=['CO'], n_threads=30)"
qwi9 = "qwi(indicator_list=indicators, obs_level='msa', state_list=['KS'], n_threads=30)" # Problem msas
qwi10 = "qwi(indicator_list=indicators, obs_level='state', state_list=['CO', 'KY', 'HI', 'OR'], firm_char=['industry', 'firmage'], worker_char=['sex', 'agegrp'], n_threads=30)"
qwi11 = "qwi(indicator_list=indicators, obs_level='state', state_list=['CT', 'DC'], firm_char=['firmage'], worker_char=['race', 'ethnicity'], n_threads=30)"
qwi12 = "qwi(indicator_list=indicators, obs_level='state', worker_char=['education'], n_threads=30)"
qwi13 = "qwi(indicator_list=indicators, obs_level='msa', state_list=['GA', 'WY', 'TX', 'KS'], firm_char=['firmsize'], worker_char=['sex', 'education'], n_threads=30)"
qwi14 = "qwi(indicator_list=indicators, obs_level='county', state_list=['AL', 'NY'], firm_char=['industry'], worker_char=['race'], n_threads=30)"

# Call with missing counties/msa's
qwi15 = "qwi(indicator_list=indicators, obs_level='county', state_list=['AK', 'SD'], n_threads=30)"
qwi16 = "qwi(indicator_list=indicators, obs_level='msa', state_list=['KS', 'AL', 'WV'], n_threads=30)"
qwi17 = "qwi(indicator_list=indicators, obs_level='county', worker_char=['ethnicity'], n_threads=30)"
qwi18 = "qwi(indicator_list=indicators, obs_level='msa', n_threads=30)"

# Fips list examples
qwi19 = "qwi(indicator_list=indicators, obs_level='county', fips_list=counties, n_threads=30)"
qwi20 = "qwi(indicator_list=indicators, obs_level='msa', fips_list=msas, n_threads=30)"
qwi21 = "qwi(indicator_list=indicators, obs_level='county', fips_list=counties, worker_char=['ethnicity'], n_threads=30)"
qwi22 = "qwi(indicator_list=indicators, obs_level='msa', fips_list=msas, firm_char=['industry'], n_threads=30)"
qwi23 = "qwi(indicator_list=indicators, obs_level='msa', fips_list=msas, firm_char=['industry'], worker_char=['education'], n_threads=30)"

# Private examples
qwi24 = "qwi(indicator_list=indicators, obs_level='county', state_list=['VA', 'VT'], private=True, n_threads=30)"
qwi25 = "qwi(indicator_list=indicators, obs_level='msa', state_list=['IA', 'IN'], private=True, n_threads=30)"
qwi26 = "qwi(indicator_list=indicators, obs_level='state', private=True, n_threads=30)"
qwi27 = "qwi(indicator_list=indicators, obs_level='state', worker_char=['agegrp'], private=True, n_threads=30)"

# Annualize examples
qwi28 = "qwi(indicator_list=indicators, obs_level='county', annualize='April', n_threads=30)"
qwi29 = "qwi(indicator_list=indicators, obs_level='county', annualize=False, n_threads=30)"
qwi30 = "qwi(indicator_list=indicators, obs_level='msa', firm_char=['firmsize'], annualize='April', n_threads=30)"
qwi31 = "qwi(indicator_list=indicators, obs_level='msa', annualize=False, n_threads=30)"
qwi32 = "qwi(indicator_list=indicators, obs_level='state', annualize='April', n_threads=30)"
qwi33 = "qwi(indicator_list=indicators, obs_level='state', firm_char=['industry'], annualize=False, n_threads=30)"

# Strata totals examples
qwi34 = "qwi(indicator_list=indicators, obs_level='state', firm_char=['firmsize'], strata_totals=True, n_threads=30)"
qwi35 = "qwi(indicator_list=indicators, obs_level='state', worker_char=['sex'], strata_totals=True, n_threads=30)"


module_to_ntests = {
    'acs': range(1,9),
    'bed': range(1,11),
    'bds': range(1,14),
    'bfs': range(1,19),
    'pep': range(1,8),
    'qwi': range(1,36)
}


def _log(text, output_location, write_type='a'):
    """Helper fn for run_tests"""
    if output_location:
        text += '\n'
        with open(output_location, write_type) as f:
            f.write(text)
    else:
        print(text)


def run_tests(tests, output_location=None, num_retries=1):
    """
    Run tests of the kauffman library, saves output to a folder if desired.

    Parameters
    ----------
    tests: str or list
        The test or tests you would like to perform, referred to by the string
        of variable the holds the test.
        Examples: ['qwi10'], 'qwi', ['acs1', 'acs2'], 'all'
    output_location: str, optional
        The file location of the output
    num_retries : int, default 1
        If the test fails, how many times to retry
    """
    # Allow list or string
    if type(tests) != list and tests not in list(module_to_ntests) + ['all']:
        tests = [tests]

    # test ids
    if type(tests) == str and tests in module_to_ntests:
        ids = [f'{tests}{i}' for i in module_to_ntests[tests]]
    elif tests == 'all':
        ids = [f'{m}{i}' for m in module_to_ntests for i in module_to_ntests[m]]
    else:
        ids = tests

    # test strings
    if type(tests) == str and tests in module_to_ntests:
        tests = [eval(f'{tests}{n}') for n in module_to_ntests[tests]] 
    elif tests == 'all':
        tests = [eval(f'{m}{i}') for m in module_to_ntests for i in module_to_ntests[m]]
    else:
        tests = [eval(test) for test in tests]
    
    # start log
    log_path = output_location + '\log.txt' if output_location else None
    start_text = f'Started tests at {dt.now().time()}'
    _log(start_text, log_path, write_type='w')

    # perform tests
    for test, test_id in zip(tests, ids):
        success = False
        tries = 1
        _log(f'Testing: {test_id} at time {dt.now().time()}', log_path)
        while not success and tries <= num_retries + 1:
            try:
                df = eval(test)

                if output_location:
                    out_path = output_location + '/' + test_id + '.csv'
                    df.to_csv(out_path, index=False)
                success = True
                _log(f'Success at time {dt.now().time()}', log_path)

            except:
                _log(
                    f'[Try: {tries}]. FAIL at time {dt.now().time()}', 
                    log_path
                )
                tries += 1

    # end log
    _log(f'Ended testing at time {dt.now().time()}', log_path)


# run_tests('pep1')