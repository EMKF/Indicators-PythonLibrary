API_CELL_LIMIT = 400000

WORKER_CROSSES = [
    {'sex', 'agegrp'}, {'sex', 'education'}, {'education'}, 
    {'ethnicity', 'race'}, {'sex'}, {'agegrp'}, {'race'}, {'ethnicity'}, set()
]

STRATA_TO_NLEVELS = {
    'firmage':6, 'firmsize':6, 'industry':20, 'sex':3, 'agegrp':9,
    'race':8, 'ethnicity':3, 'education':6
}

GEO_TO_MAX_CARDINALITY = {
    'county': 254,
    'msa': 30,
    'state': 1
}

STRATA_TO_LEVELS = {
    'firmage': [x for x in range(0,6)],
    'firmsize': [x for x in range(0,6)],
    'industry': [
        '00', '11', '21', '22', '23', '31-33', '42', '44-45', '51', '52', '53',
        '54', '55', '56', '61', '62', '71', '72', '81', '92'
    ],
    'education': [f'E{i}' for i in range(0,6)],
    'sex': [x for x in range(0,3)],
    'agegrp':[f'A0{i}' for i in range(0,9)],
    'race':[f'A{i}' for i in range(0,8) if i != 6],
    'ethnicity':[f'A{i}' for i in range(0,3)]
}

OUTCOMES = [
    'EarnBeg', 'EarnHirAS', 'EarnHirNS', 'EarnS', 'EarnSepS', 'Emp', 'EmpEnd',
    'EmpS', 'EmpSpv', 'EmpTotal', 'FrmJbC', 'FrmJbCS', 'FrmJbGn', 'FrmJbGnS',
    'FrmJbLs', 'FrmJbLsS', 'HirA', 'HirAEnd', 'HirAEndR', 'HirAEndRepl',
    'HirAEndReplr', 'HirAs', 'HirN', 'HirNs', 'HirR', 'Payroll', 'Sep',
    'SepBeg', 'SepBegR', 'SepS', 'SepSnx', 'TurnOvrS'
]

MISSING_COUNTIES = {
    '02': ['063', '066', '158'], 
    '46': ['102']
}

MISSING_MSAS = {
    '01': ['10760', '12120', '21640', '22840', '27530'],
    '13': ['21640'],
    '04': ['39150'],
    '05': ['26260'],
    '17': ['36837'],
    '18': ['14160', '42500'],
    '19': ['16140', '37800'],
    '20': ['49060'],
    '21': ['16420'],
    '22': ['27660', '33380'],
    '27': ['21860', '24330'],
    '28': ['48500'],
    '33': ['30100'],
    '35': ['40760'],
    '36': ['39100'],
    '39': ['19430'],
    '42': ['41260'],
    '45': ['46420'],
    '47': ['15140'],
    '48': ['14300', '24180', '37770', '40530'],
    '50': ['30100'],
    '54': ['34350']
}

WORKER_CHAR_ERROR = 'Invalid input to worker_char. See function documentation' \
    ' for valid groups.'

FIRM_CHAR_ERROR = 'Invalid input to firm_char. Can only specify one of ' \
    'firmage or firmsize.'

FIPS_LIST_ERROR = 'If fips_list is provided, geo_level must be either msa or ' \
    'county.'

PRIVATE_FIRM_CHAR_WARN = 'Warning: Firmage, firmsize only available when ' \
    'private = True. Variable private has been set to True.'

PRIVATE_US_WARN = 'Warning: US-level data is only available when ' \
    'private=True. Variable "private" has been set to True.'