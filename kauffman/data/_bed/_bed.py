import pandas as pd
import kauffman.constants as c
from ._firm_size import firm_size_data
from ._est_age_surv import est_age_surv_data


def bed(series, table, obs_level='us', state_list='all', industry='00'):
    """
       todo: go through this doc string
    'https://www.bls.gov/bdm/us_age_naics_00_table1.txt
    'https://www.bls.gov/bdm/age_by_size/age_naics_base_ein_20201_t4.xlsx (firm)
    'https://www.bls.gov/bdm/age_by_size/age_naics_base_20201_t4.xlsx 
        (establishment)


        # todo...series and table need to be lumped together somehow
       BED series is bdm (Establishment Age and Survival Data). 
       Industry is 00, All.
        series: str
            'bdm: firm size'
            'bdm: establishment age and survival'

        table: int,
            1: Private sector gross jobs gains and losses by establishment age
            2: Private sector gross jobs gains and losses, as a percent of
                employment, by establishment age
            3: Number of private sector establishments by direction of
                employment change, by establishment age
            4: Number of private sector establishments by direction of
                employment change by establishment age, as a percent of total
                establishments
            5: Number of private sector establishments by age
            6: Private sector employment by establishment age
            7: Survival of private sector establishments by opening year
            1bf: Annual gross job gains and gross job losses by age and base
                size of firm

        obs_level: str
            'us'
            'state'

        state_list: 'all' or list
            When obs_level is state, the list of states (in postal code
            abbreviation format) to include in the data pull

        industry: str, NAICS codes
            00: All
            11: Agriculture, forestry, fishing, and hunting
            21: Mining, quarrying, and oil and gas extraction
            22: Utilities
            23: Construction
            31: Manufacturing
            42: Wholesale trade
            44: Retail trade
            48: Transportation and warehousing
            51: Information
            52: Finance and insurance
            53: Real estate and rental and leasing
            54: Professional, scientific, and technical services
            55: Management of companies and enterprises
            56: Administrative and waste services
            61: Educational services
            62: Health care and social assistance
            71: Arts, entertainment, and recreation
            72: Accommodation and food services
            81: Other services (except public administration)
    """
    # todo: integrate this somehow above
    """
    Private Sector Firm-Level Job Gains and Losses

    table:
        1: Seasonally Adjusted
        2: Not Seasonally Adjusted
        3: As % Employment Seasonally Adjusted
        4: As % Employment Not Seasonally Adjusted
    firm size: (number of employees)
        1: 1-4 , 2: 5-9 , 3: 10-19 , 4: 20-49 , 5: 50-99, 6: 100-249 ,
        7: 250-499 , 8: 500-999 , 9: >1000

    """
    state_list = c.STATES if state_list == 'all' else state_list    
    region_list = state_list if obs_level == 'state' else ['us']

    if series == 'firm size' or 'size':
        return pd.concat([
                firm_size_data(table, size)
                for size in range(1, 10)
            ])
    elif series == 'establishment age and survival' or 'age':
        return pd.concat([
                est_age_surv_data(table, region, industry)
                for region in region_list
            ])