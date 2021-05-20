from re import S
import pandas as pd
import kauffman.constants as c
from kauffman.data.helpers.bed_helpers.firm_size_helpers import _firm_size_data_create
from kauffman.data.helpers.bed_helpers.est_age_surv_helpers import _est_age_surv_data_create
from kauffman.data.helpers import _acs_data_create, _bds_data_create, _bfs_data_create, _pep_data_create, _qwi_data_create


# todo: updates (1) move the column and renaming lines to _helpers files and reindenxing.
# todo: mostly the code in each of these is the same...so can consolidate that

def acs(series_lst):
    """
        https://api.census.gov/data/2019/acs/acs1/variables.html

        'B24081_001E': 'total',
        'B24081_002E': 'private',
        'B24081_003E': 'private_employee',
        'B24081_004E': 'private_self_employed',
        'B24081_005E': 'non_profit',
        'B24081_006E': 'local_government',
        'B24081_007E': 'state_government',
        'B24081_008E': 'federal_government',
        'B24081_009E': 'self_employed_not_inc',
        'B24092_001E': 'total_m',
        'B24092_002E': 'private_m',
        'B24092_003E': 'private_employee_m',
        'B24092_004E': 'private_self_employed_m',
        'B24092_005E': 'non_profit_m',
        'B24092_006E': 'local_government_m',
        'B24092_007E': 'state_government_m',
        'B24092_008E': 'federal_government_m',
        'B24092_009E': 'self_employed_not_inc_m',
        'B24092_010E': 'total_f',
        'B24092_011E': 'private_f',
        'B24092_012E': 'private_employee_f',
        'B24092_013E': 'private_self_employed_f',
        'B24092_014E': 'non_profit_f',
        'B24092_015E': 'local_government_f',
        'B24092_016E': 'state_government_f',
        'B24092_017E': 'federal_government_f',
        'B24092_018E': 'self_employed_not_inc_f'
    """
    return _acs_data_create(series_lst)


def bed(series, table, obs_level='all', industry='00'):
    """
       todo: go through this doc string
    'https://www.bls.gov/bdm/us_age_naics_00_table1.txt
    'https://www.bls.gov/bdm/age_by_size/age_naics_base_ein_20201_t4.xlsx  # firm
    'https://www.bls.gov/bdm/age_by_size/age_naics_base_20201_t4.xlsx  # establishment


        # todo...series and table need to be lumped together somehow
       BED series is bdm (Establishment Age and Survival Data). Industry is 00, All.
        series: str
            'bdm: firm size'
            'bdm: establishment age and survival'

        table: int,
            1: Private sector gross jobs gains and losses by establishment age
            2: Private sector gross jobs gains and losses, as a percent of employment, by establishment age
            3: Number of private sector establishments by direction of employment change, by establishment age
            4: Number of private sector establishments by direction of employment change by establishment age, as a percent of total establishments
            5: Number of private sector establishments by age
            6: Private sector employment by establishment age
            7: Survival of private sector establishments by opening year
            1bf: Annual gross job gains and gross job losses by age and base size of firm

        obs_level: str
            'all'
            'us'
            'state'
            state abbreviation code

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
        1: Seasonally Adjusted, 2: Not Seasonally Adjusted, 3: As % Employment Seasonally Adjusted, 4: As % Employment Not Seasonally Adjusted
    firm size: (number of employees)
        1: 1-4 , 2: 5-9 , 3: 10-19 , 4: 20-49 , 5: 50-99, 6: 100-249 , 7: 250-499 , 8: 500-999 , 9: >1000

    """

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level.lower() == 'state':
            region_lst = c.states
        elif obs_level.lower() == 'all':
            region_lst = ['us'] + c.states
        else:
            region_lst = [obs_level.lower()]

    if series == 'firm size':
        return pd.concat(
                [
                    _firm_size_data_create(table, size)
                    for size in range(1, 10)
                ],
                axis=0
            )
    elif series == 'establishment age and survival':
        return pd.concat(
                [
                    _est_age_surv_data_create(table, region.lower(), industry)
                    for region in region_lst
                ],
                axis=0
            )


def bds(series_lst, obs_level='all'):
    """ Create a pandas data frame with results from a BDS query. Column order: fips, region, time, series_lst.

    Keyword arguments:

    series_lst-- lst of variables to pull; see https://www.census.gov/content/dam/Census/programs-surveys/business-dynamics-statistics/BDS_Codebook.pdf or https://api.census.gov/data/timeseries/bds/variables.html

        CBSA: Geography
        COUNTY: Geography
        DENOM: (DHS) denominator
        EAGE: Establishment age code
        EMP: Number of employees
        EMPSZES: Employment size of establishments code
        EMPSZESI: Initial employment size of establishments code
        EMPSZFI: Employment size of firms code
        EMPSZFII: Initial employment size of firms code
        ESTAB: Number of establishments
        ESTABS_ENTRY: Number of establishments born during the last 12 months
        ESTABS_ENTRY_RATE: Rate of establishments born during the last 12 months
        ESTABS_EXIT: Number of establishments exited during the last 12 months
        ESTABS_EXIT_RATE: Rate of establishments exited during the last 12 months
        FAGE: Firm age code
        FIRM: Number of firms
        FIRMDEATH_EMP: Number of employees associated with firm deaths during the last 12 months
        FIRMDEATH_ESTABS: Number of establishments associated with firm deaths during the last 12 months
        FIRMDEATH_FIRMS: Number of firms that exited during the last 12 months
        GEO_ID: Geographic identifier code
        GEOCOMP: GEO_ID Component
        INDGROUP: Industry group
        INDLEVEL: Industry level
        JOB_CREATION: Number of jobs created from expanding and opening establishments during the last 12 months
        JOB_CREATION_BIRTHS: Number of jobs created from opening establishments during the last 12 months
        JOB_CREATION_CONTINUERS: Number of jobs created from expanding establishments during the last 12 months
        JOB_CREATION_RATE: Rate of jobs created from expanding and opening establishments during the last 12 months
        JOB_CREATION_RATE_BIRTHS: Rate of jobs created from opening establishments during the last 12 months
        JOB_DESTRUCTION: Number of jobs lost from contracting and closing establishments during the last 12 months
        JOB_DESTRUCTION_CONTINUERS: Number of jobs lost from contracting establishments during the last 12 months
        JOB_DESTRUCTION_DEATHS: Number of jobs lost from closing establishments during the last 12 months
        JOB_DESTRUCTION_RATE: Rate of jobs lost from contracting and closing establishments during the last 12 months
        JOB_DESTRUCTION_RATE_DEATHS: Rate of jobs lost from closing establishments during the last 12 months
        METRO: Establishments located in Metropolitan or Micropolitan Statistical Area indicator
        NAICS: 2012 NAICS Code
        NATION: Geography
        NET_JOB_CREATION: Number of net jobs created from expanding/contracting and opening/closing establishments during the last 12 months
        NET_JOB_CREATION_RATE: Rate of net jobs created from expanding/contracting and opening/closing establishments during the last 12 months
        REALLOCATION_RATE: Rate of reallocation during the last 12 months
        SECTOR: NAICS economic sector
        STATE: Geography
        SUBSECTOR: Subsector
        SUMLEVEL: Summary Level code
        ucgid: Uniform Census Geography Identifier clause
        YEAR: Year


            FAGE codes
                1	Total	0	All firm ages
            10	0 Years	1	Firms less than one year old
            20	1 Year	1	Firms one year old
            25	1-5 Years	2	Firms between one and five years old
            30	2 Years	1	Firms two years old
            40	3 Years	1	Firms three years old
            50	4 Years	1	Firms four years old
            60	5 Years	1	Firms five years old
            70	6-10 Years	0	Firms between six and ten years old
            75	11+ Years	2	Firms eleven or more years old
            80	11-15 Years	1	Firms between eleven and fifteen years old
            90	16-20 Years	1	Firms between sixteen and twenty years old
            100	21-25 Years	1	Firms between twenty one and twenty five years old
            110	26+ Years	1	Firms twenty six or more years old
            150	Left Censored	0	"Firms of unknown age (born before 1977)”

    obs_level-- str or lst of the level of observation(s) to pull at.
            all:
            us:
            state:
            county:
            list of regions according to fips code

    first year available is 1978, last year is 2018
    """
    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level in ['us', 'state', 'county']:
            region_lst = [obs_level]
        else:
            region_lst = ['us', 'state', 'county']

    return pd.concat(
            [
                _bds_data_create(series_lst, region)
                for region in region_lst
            ],
            axis=0
        )


def bfs(series_lst, obs_level='all', seasonally_adj=True, annualize=False, march_shift=False):
    """ Create a pandas data frame with results from a BFS query. Column order: fips, region, time, series_lst.


    Keyword arguments:
    series_lst-- lst of variables to be pulled.

        Variables:
            BA_BA: 'Business Applications'
            BA_CBA: 'Business Applications from Corporations'
            BA_HBA: 'High-Propensity Business Applications'
            BA_WBA: 'Business Applications with Planned Wages'
            BF_BF4Q: 'Business Formations within Four Quarters'
            BF_BF8Q: 'Business Formations within Eight Quarters'
            BF_PBF4Q: Projected Business Formations within Four Quarters
            BF_PBF8Q: Projected Business Formations within Eight Quarters
            BF_SBF4Q: Spliced Business Formations within Four Quarter
            BF_SBF8Q: Spliced Business Formations within Eight Quarters
            BF_DUR4Q: Average Duration (in Quarters) from Business Application to Formation within Four Quarters
            BF_DUR8Q: Average Duration (in Quarters) from Business Application to Formation within Eight Quarters

    obs_level-- The level to pull observations for. ('state', 'us', or 'all')


    seasonally_adj-- Option to use the census adjustment for seasonality and smooth the time series. (True or False)

    annualize-- Aggregates across months and annulizes data. (True or False)

    march_shift-- When True the year end is March, False the year end is December. (True or False)



    """

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level == 'us':
            region_lst = ['US']
        elif obs_level == 'state':
            region_lst = c.states
        else:
            region_lst = ['US'] + c.states

    return pd.concat(
            [
                _bfs_data_create(region, series_lst, seasonally_adj, annualize, march_shift)
                for region in region_lst
            ],
            axis=0
        )


def pep(obs_level='all'):
    """ Create a pandas data frame with results from a PEP query. Column order: fips, region, time, POP.

    Collects nation- and state-level population data, similar to https://fred.stlouisfed.org/series/CAPOP, from FRED. Requires an api key...
    register here: https://research.stlouisfed.org/useraccount/apikey. For now, I'm just including my key until we
    figure out the best way to do this.

    #todo travis edit ^

    Collects county-level population data from the Census API:

    (as of 2020.03.16)

    Keyword arguments:

    obs_level-- str of the level of observation to pull from.
        'state': resident population of state from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019

    """
    # todo: do we want to allow user to filter by year? if not, remove these two parameters.

    if type(obs_level) == list:
        region_lst = obs_level
    else:
        if obs_level in ['us', 'state', 'msa', 'county']:
            region_lst = [obs_level]
        else:
            region_lst = ['us', 'state', 'msa', 'county']

    return pd.concat(
            [
                _pep_data_create(region)
                for region in region_lst
            ],
            axis=0
        )


def qwi(indicator_lst='all', obs_level='all', state_list='all', private=False, annualize='January', strata=[]):
    # todo: I don't think MSA and state_list will work, because of the issue of MSAs crossing state lines. Is there a way around this?
    """
    Fetches nation-, state-, MSA-, or county-level Quarterly Workforce Indicators (QWI) data either from the LED
    extractor tool in the case of national data (https://ledextract.ces.census.gov/static/data.html) or from the
    Census's API in the case of state, MSA, or county (https://api.census.gov/data/timeseries/qwi/sa/examples.html).

    obs_level: str, lst
        'state': resident population of state from 1990 through 2019
        'msa': resident population of msa from 1990 through 2019
        'county': resident population of county from 1990 through 2019
        'us': resident population in the united states from 1959 through 2019
        'all': default, returns data on all of the above observation levels

    indicator_lst: str, lst
        'all': default, will return all QWI indicaotrs;
        otherwise: return list of indicators plus 'time', 'ownercode', 'firmage', and 'fips'

        EmpSpv: Full-Quarter Employment in the Previous Quarter: Counts
        SepBeg: Beginning-of-Quarter Separations
        EmpS: Full-Quarter Employment (Stable): Counts
        FrmJbLsS: Firm Job Loss (Stable): Counts
        HirAEndReplr: Replacement Hiring Rate
        HirAEnd: End-of-Quarter Hires
        FrmJbLs: Firm Job Loss: Counts (Job Destruction)
        EarnS: Full Quarter Employment (Stable): Average Monthly Earnings
        HirR: Hires Recalls: Counts
        FrmJbC: Firm Job Change:Net Change
        Emp: Beginning-of-Quarter Employment: Counts
        FrmJbGnS: Firm Job Gains (Stable): Counts
        HirAs: Hires All (Stable): Counts (Flows into Full-QuarterEmployment)
        SepSnx: Separations (Stable), Next Quarter: Counts (Flow out of Full-Quarter Employment)
        HirNs: Hires New (Stable): Counts (New Hires to Full-Quarter Status)
        Sep: Separations: Counts
        EarnHirAS: Hires All (Stable): Average Monthly Earnings
        Payroll: Total Quarterly Payroll: Sum
        HirA: Hires All: Counts (Accessions)
        FrmJbCS: Job Change (Stable): Net Change
        EmpTotal: Employment-Reference Quarter: Counts
        HirAEndRepl: Replacement Hires
        EarnHirNS: Hires New (Stable): Average Monthly Earnings
        TurnOvrS: Turnover (Stable)
        HirN: Hires New: Counts
        EarnBeg: End-of-Quarter Employment: Average Monthly Earnings
        EmpEnd: End-of-Quarter Employment: Counts
        SepBegR: Beginning-of-Quarter Separation Rate
        EarnSepS: Separations (Stable): Average Monthly Earnings
        HirAEndR: End-of-Quarter Hiring Rate
        SepS: Separations (Stable): Counts (Flow out of Full-Quarter Employment)
        FrmJbGn: Firm Job Gains: Counts (Job Creation)

    state_list: str, lst
        'all': default, includes all US states and D.C.
        otherwise: a state or list of states, identified using postal code abbreviations

    private: bool
        True: All private only
        False: All
        if by_age_size is not None, then private is set to True

    annualize: None, str
        'None': leave as quarterly data
        'January': annualize using Q1 as beginning of year
        'March': annualize using Q2 as beginning of year

    strata: lst, str
        empty: default
        'firmage': stratify by age
        'firmsize': stratify by size
        'sex': stratify by gender
        'industry': stratify by industry, NAICS 2-digit
    """
    if obs_level in ['us', 'state', 'county', 'msa']:
        region_lst = [obs_level]
    elif obs_level == 'all':
        region_lst = ['us', 'state', 'county', 'msa']
    else:
        print('Invalid input to obs_level.')

    if state_list == 'all':
        state_list = c.states
    elif type(state_list) == str:
        state_list = [state_list]

    state_list = [c.state_abb_fips_dic[s] for s in state_list]

    if indicator_lst == 'all':
        indicator_lst = c.qwi_outcomes
    elif type(indicator_lst) == str:
        indicator_lst = [indicator_lst]

    strata = [strata] if type(strata) == str else strata
    private = True if any(x in ['firmage', 'firmsize'] for x in strata) else private

    return pd.concat(
            [
                _qwi_data_create(indicator_lst, region, state_list, private, annualize, strata)
                for region in region_lst
            ],
            axis=0
        )
