ALL_VARIABLES = [
    'DENOM', 'EMP', 'ESTAB', 'ESTABS_ENTRY', 'ESTABS_ENTRY_RATE', 'ESTABS_EXIT', 'ESTABS_EXIT_RATE', 'FIRM', 'FIRMDEATH_EMP', 'FIRMDEATH_ESTABS', 
    'FIRMDEATH_FIRMS', 'JOB_CREATION', 'JOB_CREATION_BIRTHS', 
    'JOB_CREATION_CONTINUERS', 'JOB_CREATION_RATE', 'JOB_CREATION_RATE_BIRTHS', 'JOB_DESTRUCTION', 'JOB_DESTRUCTION_CONTINUERS', 'JOB_DESTRUCTION_DEATHS', 
    'JOB_DESTRUCTION_RATE', 'JOB_DESTRUCTION_RATE_DEATHS',
    'NET_JOB_CREATION', 'NET_JOB_CREATION_RATE', 'REALLOCATION_RATE'
]

VALID_CROSSES = [
    {'FAGE'}, {'EAGE'}, {'EMPSZFI'}, {'EMPSZES'}, {'EMPSZFII'}, {'EMPSZESI'},
    {'GEOCOMP', 'METRO'}, {'STATE'}, {'COUNTY'}, {'MSA'}, {'NAICS'}, 
    {'EMPSZFI', 'FAGE'}, {'EMPSZFII', 'FAGE'}, {'EAGE', 'EMPSZES'}, 
    {'EAGE', 'EMPSZESI'}, {'NAICS', 'STATE'}, {'GEOCOMP', 'METRO', 'STATE'},
    {'FAGE', 'STATE'}, {'EMPSZFI', 'STATE'}, {'EMPSZFII', 'STATE'}, 
    {'EAGE', 'STATE'}, {'EMPSZES', 'STATE'}, {'EMPSZESI', 'STATE'}, 
    {'FAGE', 'NAICS'}, {'EMPSZFI', 'NAICS'}, {'EMPSZFII', 'NAICS'},
    {'EAGE', 'NAICS'}, {'EMPSZES', 'NAICS'}, {'EMPSZESI', 'NAICS'},
    {'FAGE', 'GEOCOMP', 'METRO'}, {'EMPSZFI', 'GEOCOMP', 'METRO'},
    {'EMPSZFII', 'GEOCOMP', 'METRO'}, {'EAGE', 'GEOCOMP', 'METRO'},
    {'EMPSZES', 'GEOCOMP', 'METRO'}, {'EMPSZESI', 'GEOCOMP', 'METRO'}, 
    {'GEOCOMP', 'METRO', 'NAICS'}, {'FAGE', 'MSA'}, {'EMPSZFI', 'MSA'}, 
    {'EMPSZFII', 'MSA'}, {'EAGE', 'MSA'}, {'MSA', 'NAICS'}, {'COUNTY', 'FAGE'},
    {'COUNTY', 'EMPSZFI'}, {'COUNTY', 'EMPSZFII'}, {'COUNTY', 'EAGE'},
    {'COUNTY', 'NAICS'}, {'FAGE', 'MSA', 'NAICS'}, {'EMPSZFI', 'MSA', 'NAICS'},
    {'EMPSZFII', 'MSA', 'NAICS'}, {'EAGE', 'MSA', 'NAICS'}, 
    {'FAGE', 'NAICS', 'STATE'}, {'EMPSZFI', 'NAICS', 'STATE'}, 
    {'EMPSZFII', 'NAICS', 'STATE'}, {'EAGE', 'NAICS', 'STATE'},
    {'FAGE', 'GEOCOMP', 'METRO', 'NAICS'}, {'EMPSZFII', 'FAGE', 'NAICS'},
    {'EMPSZFI', 'GEOCOMP', 'METRO', 'NAICS'}, 
    {'EMPSZFII', 'GEOCOMP', 'METRO', 'NAICS'},
    {'EAGE', 'GEOCOMP', 'METRO', 'NAICS'}, {'FAGE', 'GEOCOMP', 'METRO', 'STATE'},
    {'EMPSZFI', 'GEOCOMP', 'METRO', 'STATE'}, 
    {'EMPSZFII', 'GEOCOMP', 'METRO', 'STATE'}, 
    {'EAGE', 'GEOCOMP', 'METRO', 'STATE'}, 
    {'GEOCOMP', 'METRO', 'NAICS', 'STATE'}, {'EMPSZFI', 'FAGE', 'NAICS'},
    {'FAGE', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'},
    {'EMPSZFI', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'}, 
    {'EMPSZFII', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'}, 
    {'EAGE', 'GEOCOMP', 'METRO', 'NAICS', 'STATE'}
]


def check_strata_valid(geo_level, strata):
    if not strata:
        valid = True
    elif geo_level in ['state', 'county', 'msa']:
        strata = set(strata + [geo_level.upper()])
        valid = strata in VALID_CROSSES
    elif geo_level == 'all':
        valid = all(
            set(strata + [o.upper()]) in VALID_CROSSES 
            for o in ['us', 'state', 'msa', 'county']
        )
    else:
        strata = set(strata)
        valid = strata in VALID_CROSSES
    
    if not valid:
        raise Exception(
            f'This is not a valid combination of strata for geo_level ' \
            f'{geo_level}. See ' \
            'https://www.census.gov/data/datasets/time-series/econ/bds/bds-datasets.html' \
            ' for a list of valid crossings.'
        )