from itertools import product


AGE_CODE_TO_LABEL = {
    0: 'Less than one year old',
    1: '1 to 4 years',
    2: '5 to 9 years',
    3: '10 years or older',
    4: 'All'
}

SIZE_CODE_TO_LABEL = {
    0: '1 to 4 employees',
    1: '5 to 9 employees',
    2: '10 to 19 employees',
    3: '20 to 49 employees',
    4: '50 to 99 employees',
    5: '100 to 499 employees',
    6: '500 or more employees',
    7: 'All'
}

AGE_SIZE_LIST = list(product(
    AGE_CODE_TO_LABEL.keys(), 
    SIZE_CODE_TO_LABEL.keys()
))

TABLE1BF_COLS = [
    'time', 'firms', 'establishments', 'net_change', 'total_job_gains',
    'expanding_job_gains', 'opening_job_gains', 'total_job_losses',
    'contracting_job_losses', 'closing_job_losses'
]

TABLE_FIRM_SIZE_COLS = [
    'time', 'quarter', 'net_change', 'total_job_gains',
    'expanding_firms_job_gains', 'opening_firms_job_gains', 'total_job_losses',
    'contracting_firms_job_losses', 'closing_firms_job_losses'
]

SIZE_CODE_TO_LABEL2 = {
    1: '1 to 4 employees',
    2: '5 to 9 employees',
    3: '10 to 19 employees',
    4: '20 to 49 employees',
    5: '50 to 99 employees',
    6: '100 to 249 employees',
    7: '250 to 499 employees',
    8: '500 to 999 employees',
    9: '1000 or more employees'
}

MONTH_TO_QUARTER = {
    'March': 1,
    'June': 2,
    'September': 3,
    'December': 4
}