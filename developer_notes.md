_These notes are primarily directed to a future data engineer/repository maintainer._

_Created: 12/5/22_

_Last edited: 2/18/23_

<br>

# Introduction/General Notes

As stated in the README, the purpose of this repository is to house code to fetch and clean commonly used publicly available datasets relating to entrepreneurship. Specifically, it holds the **kauffman** library. It was created to fetch data used in the generation of the Kauffman Indicators (see: [kese](https://github.com/EMKF/kese), [eji](https://github.com/EMKF/eji), [neb](https://github.com/EMKF/neb)), but can fetch a much wider range of data as well. The repository's intent is twofold: to provide transparency and replicability for the data collection process and to create a flexible tool for future projects.

The goal of this repository is to make the code stable enough that even non-programmers could generate the Indicators each year, although it will likely require maintenance due to changes in the Census user interface. One thing to watch for is increased offerings through the API. The API has been the most stable platform for fetching data, so the more we can transfer our code to fetch from the API, the better.

This document provides an introduction to the repository as well as some background on the data sources and quirks of the code.

## General structure

### kauffman
The main product of this library is the set of data-fetching functions contained in `kauffman > data_fetch`. Each function is in a separate file with a name matching the function, except prefixed by an underscore. Some of these files reside within their own folders, by the same name as the main file. (Ex: `kauffman > data_fetch > _qwi > _qwi.py` contains function `qwi`.) These folders also contain other helper files. 

This library also contains a series of tools, organized within a few files found in `kauffman > tools`. These tools are functions which are intended to be capable of standalone usage; however, most of them are also used within the data-fetching functions. They are split between `api_tools.py`, which contains shared functions used across modules that fetch from the Census API; `general_tools.py`, which contain general-purpose functions used across many modules, and `qwi_tools.py`, which contain QWI-specific tools that could be useful outside of the creation of QWI data.

The last component of the kauffman library is the constants file, which contains constant variables that are used across multiple modules.

### tests
The tests folder contains a series of calls of the functions that should be able to run. Its purpose is to: (a) provide a test suite to be used for comparison before pushing changes to the master branch, and (b) provide the public some usage examples for the kauffman library. It currently just has `data_tests.py`, which tests the functions in `kauffman > data`. One future task might be to create a file called tools_tests.py, which would test the functions in `kauffman > tools`.

### Other files
Most of the other files (the README, LICENSE, and .gitignore for example) are self-explanatory, but here are a couple notes:
* `setup.py`
    * If someone is reading this, you'll likely need to update the maintainer and maintainer_email fields.
    * I've used a casual version numbering system (A.B.C), where:
        * C represents minor changes with no impact on user experience,
        * B represents larger structural changes with limited impact on user experience, and
        * A represents major structural changes with significant impact on usage.
* `version_log.txt`
    * This is a log of what has changed with each version update of the master branch.


## Library Usage
The data package contains the following functions: `acs`, `bed`, `bds`, `bfs`, `pep`, and `qwi`. As an example, you would import `qwi` in the following manner:
```
from kauffman.data import qwi
```
You can find a list of tools functions in the README. You would import CBSA_crosswalk in the following way:
```
from kauffman.tools import CBSA_crosswalk 
```

## The Census API
With the exception of `bed`, whose data comes from the Bureau of Labor Statistics, all of our data comes from the U.S. Census, most of which pull (at least some of) their data from the Census API. To work on the code, you’ll need basic familiarity with APIs. The Census API is straightforward--‘get’ requests are written as URLs that you can look at directly in your browser. It returns a JSON that organizes the data as a list of lists, which makes the code simple. If you are fetching url ‘www.census.api.gov/data/just-an-example.html’, you would do it in the following way:
```
import pandas as pd
import requests

r = requests.get(www.census.api.gov/data/just-an-example.html)
data = pd.DataFrame(r.json())
```
Additional notes:
* **Location:** You can see all of the APIs at https://api.census.gov/data.html, or you can refer to the function notes (in the “Data Fetching Function Notes” section of this document) or the function docstrings for links to the more specific locations of the data.
* **Useful links:** I’ve put the links for the function-specific documentation down in the respective function notes, but here is a general link to the documentation for all of the datasets: https://www.census.gov/data/developers/data-sets.html
* **API functions:** It made sense to share certain helper functions for fetching/cleaning data from the API across data-fetching functions--hence the `api_tools.py` file in `kauffman > tools`.

## Developer to-do
* The cleanup branch is ready with changes that will affect kese, eji, and neb. It will need to be merged into downwardata’s master branch at the same time as the developer branches of kese, eji and neb are merged into their respective master branches. 
    * Waiting on: eji needs a fresh data pull, which needs to happen after the Census finishes its data updates.
* Figure out if `pep` is using consistent measures of population across all of its functions
* Consider either getting rid of `acs` or using it for something
* Figure out a long-term solution to the `qwi` release dates issue (`qwi` pulls from the Census’s API, which comes from the latest_release folder in the Census’ raw data--which means it could be from inconsistent releases. Temporary solution is to raise an error if the user tries to pull `qwi` data when the Census is in the middle of an update, but this is not ideal)
* Create tests for the tools
* Create docstrings for the tools
* Resolve issue with `tools > aggregate_county_to_msa`: Need to enforce that all counties are present before summing to get the msa-level data

<br>

# Data Fetching Function Notes

## `acs`
* **Used in:** None. It’s not used in any of the Indicators calculations currently. I am not sure why it was created--it was from before my time, and I left it there in case it might be useful someday. It might make sense to delete at some future date.
* **Useful links**
    * API documentation: https://www.census.gov/data/developers/data-sets/acs-1year.html
    * API 2010 base url (other years have parallel urls, just with the year replaced): https://api.census.gov/data/2010/acs/acs1.htmls
* **API quirk:** Unlike the QWI API, you cannot select all MSAs within a given state in the ACS API. Thus, as it notes in the docstring, state_list is not available for geo_level = ‘msa’.

## `bed`
* **Used in:** kese
* **Structure:** This function is divided into three files: the main function is in `_bed.py`, and it retrieves either "establishment age and survival" or "firm size" series, which are stored in separate files `_est_age_surv.py` and `_firm_size.py` respectively.
* **Data Sources:** 
    * Firm size: https://www.bls.gov/bdm/bdmfirmsize.htm
    * Establishment Age and Survival: https://www.bls.gov/bdm/bdmage.htm
* **To do:** This code is pretty messy--need to clean up at some point
* **IMPORTANT NOTE:** The code seems to randomly break sometimes--it will say "link cannot be found" one day and then work just fine on another day. However, the code for table1bf is not working, and it currently looks like Table 1-B-F might no longer be produced--if so, this majorly impacts our KESE repo. Need to look into this/verify/figure out solution if so.

## `bds` 
* **Used in:** neb
* **Structure:** This function can be found within `kauffman > data_fetch > _bds > _bds.py`. The _bds folder also contains a `_helpers.py` file that contains BDS-specific constants and helper functions.
* **Useful links:**
    * API documentation: https://www.census.gov/data/developers/data-sets/business-dynamics.html
    * API base url: https://api.census.gov/data/timeseries/bds.html
    * BDS datasets (shows all valid crosses of the variables): https://www.census.gov/data/datasets/time-series/econ/bds/bds-datasets.html
* **Information found about strata vs regular variables:** See 'BDS Variables Classification' subpage on https://www.census.gov/data/developers/data-sets/business-dynamics.html
    * You can sometimes add "_LABEL" at the end of a categorical variable to get the label 
    * You can add "_F" at the end of an indicator to get data quality metrics (ie: show when it is missing) 
    * METRO must be used in conjunction with GEOCOMP to get the stratification (and vice versa)
    * NAICS must come along with INDLEVEL to get stratification on INDLEVEL 
    * Users can view industry hierarchies using SECTOR or SUBSECTOR in the “predicate” in combination with NAICS in the “for” statement. For example, "get=FIRM,NAICS&for=us:1&SECTOR=22" will return tabulations for all 2/3/4 industries that start with "22". Similarly, "get=FIRM,NAICS&for=us:1&SUBSECTOR=333", will return tabulations for all industries that start with “333” 
    * Not all crossings are available in the data--see https://www.census.gov/data/datasets/time-series/econ/bds/bds-datasets.html for all available crossings 
* **NAICS, INDLEVEL, INDGROUP, SECTOR, SUBSECTOR dynamics** 
    * It goes: INDLEVEL > SECTOR > SUBSECTOR > INDGROUP 
    * NAICS contains all of: Sector, subsector, and indgroup 
    * Each of these groups would be null for the higher hierarchy 
    * Only the overall NAICS sectors are available on non-US level data 
    * Thus, INDLEVEL, INDGROUP, SECTOR, and SUBSECTOR don't do anything on non-us level data 
    * They never stratify even on US-level data--they must be used with NAICS
* **Info on null values** 
    * The flag will return “null”, “(D)”, “(S)”, or “(X)”. A “null” value means the cell is not suppressed. Cells suppressed due to containing too few firms will return a “(D)” flag. Cells suppressed due to data quality concerns will return a “(S)” flag. Cells that are unavailable due to structural missingness will return a “(X)” flag. Note that the data value will always return “0” when there is a “(D)”, “(S)”, or “(X)” suppression flag.
* **Miscellaneous:**
    * You can't just use a STRATA_LABEL variable in place of a STRATA variable and have it stratify with other variables--it must be used with STRATA
    * INDLEVEL allows you to get all 2-digit, 3-digit, or 4-digit NAICS codes

## `bfs`
* **Used in:** neb
* **Useful links:**
    * Page data came from: https://www.census.gov/econ/bfs/current/
    * Data file: https://www.census.gov/econ/bfs/csv/bfs_monthly.csv
* **Data source context:** Some of the BFS data is actually available through the API at: https://api.census.gov/data/timeseries/eits/bfs.html; however only US-level data is currently available. So we go through an alternative file that includes state-level data. But if the Census ever expands to state-level BFS data in their API, it would be better to refactor the code to go through the API.
* **Code quirk:** Seasonally adjusted data is not available for DUR variables, so the code just subs in non-seasonally adjusted values for those when seasonally_adj is True and DUR variables are included. I am not sure what the reasoning behind this decision was--it might make sense to simply disallow DUR variables when seasonally_adj is True in the future.
* **Annualizing DUR variables:** Since DUR variables are averaged outcomes, you can’t just do a simple groupby + sum to annualize them. However, since we know the denominator, we can still annualize these variables by multiplying by the denominator, doing the normal groupby + sum, and then re-averaging.

## `pep`
* **Used in:** kese, neb, eji
* **Useful links:**
    * (Non-API) data: https://www2.census.gov/programs-surveys/popest
    * The base urls vary--see the code for specifics. Start here: https://api.census.gov/data.html, and see the "pep > population" and "pep > int_population" datasets
    * API documentation: https://www.census.gov/data/developers/data-sets/popest-popproj/popest.html
* **Data sources:** Fetches from the API where possible (generally 2000 onwards), and from the following link otherwise: https://www2.census.gov/programs-surveys/popest
* **Output:** The July population estimates of each year
* **Note:** Sometimes there are multiple Census July population estimates for the same year--not clear on details, but it has something to do with when the estimate was created
    * To do: Figure out whether we have a consistent release date for the figures we are fetching

## `qwi`
* **Used in**: eji
* **Structure:** This function can be found within `kauffman > data_fetch > _qwi > _qwi.py`. The _qwi folder also contains a `_qwi_constants.py` file that contains QWI-specific constants, and a `_scrape_led.py` file that holds a function that scrapes the LED Extraction Tool for US-level data.
* **Useful links:**
    * API documentation: https://www.census.gov/data/developers/data-sets/qwi.html
    * API base url: https://api.census.gov/data/timeseries/qwi.html
    * Loading status (which years/quarters each state’s data is available for): https://lehd.ces.census.gov/applications/help/qwi_explorer.html#!loading_status
    * QWI 101: https://lehd.ces.census.gov/doc/QWI_101.pdf
    * Data Schema: https://lehd.ces.census.gov/data/schema/latest/lehd_public_use_schema.html
    * LED Extraction Tool: https://ledextract.ces.census.gov/qwi/all
* **Data sources:** The code currently uses two methods to fetch data: (1) scraping the LED Extraction Tool and (2) the Census API. The latter is preferred, but the former is also necessary since US-level data is not available on the API.
* **Cell limit:** The API has a cell limit (currently 400,000) for each query, which is based on the estimated cardinality of each variable.
    * See “Data Limitations” section of https://www.census.gov/data/developers/data-sets/qwi.html
    * Even though race doesn't include A6, I think it still assumes it is there (ie: that there are 8 levels of race) 
    * The columns are assumed to be the total number of things included in the get statement + the number of unique &'s 
* **Code structure:** Because of the cell limit, the data fetch must be split up across multiple calls in cases where the output would exceed that limit--however, since there is a huge amount of data housed in `qwi`, it is also necessary to maximize the amount of data fetched per call, in order to minimize the fetch time. This is accomplished through the choose_loops function, which calls the recursive check_loop_group function. More on this below, but the idea is that it will figure out the optimal strata to include in each loop, such that we fetch the maximum data per call without exceeding the cell limit.
* **Misc. note:** You get different US data in spite of not stratifying by worker characteristic, just depending on which database you use. Need to look into this.
* **API bugs + patch fixes:** The Census API currently has a couple bugs for `qwi` that I have requested be fixed--we need to check in on these at some point. In the meantime, I have implemented a patch fix to handle them. 
    * _Missing counties/MSAs_: To select all counties within a given state, you can normally use the "wildcard" asterisk, as in: '&county=*', but there are certain counties and MSAs that do not show up with the wildcard. The current workaround is to fetch those counties or MSAs specifically and append it to the remaining data whenever it is fetching all counties or MSAs of a state that contains those missing geographies.
    * _Industry handling_: The QWI API used to only include 2-digit NAICS codes for industry, but now includes 3- and 4- digit codes. However, we only want the 2-digit breakdown of industry. The Census API normally allows filtering by industry level, but this functionally is broken for QWI. It is possible to get multiple industries in a single call, so I've worked around this by just listing out all the 2-digit industries in the url, but due to the way the code is structured, this has led to some inelegant code.

**Function specific notes**
* `_check_loop_group`
    * As stated earlier, the goal is to maximize data per call without going over the cell limit.
    * Strategy: Recursively scan through the possible combinations of strata to include in each call (as opposed to those we are looping over--for example, if we just included the data for one industry at a time instead of all industries, "industry" would be considered "looped over"), starting with all of the strata (where there are N total strata variables), then checking all of the combinations of N - 1 strata, then N - 2 strata, and so on, and stopping when we find a combination (or combinations) that do not exceed the cell limit. At whatever cardinality of strata this is achieved, call this N - x, we check all of the combinations of N - x strata and choose the one that fetches the greatest amount of data per call. 
* `_aggregate_msas`
    * The beginning part (where it enforces msa_row_count == msa_states) is meant to ensure that all parts of the msa are present in the data before aggregating
        * The msa_states variable captures the number of states that an msa is in
        * The msa_row_count variable is the count of unique year x quarter x fips x firm_char x worker_char crosses