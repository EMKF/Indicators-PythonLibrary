# Overview
`kauffman` is a Python library for fetching and cleaning commonly used publicly available datasets related to entrepreneurship.

# Installation
In the terminal, type:
```bash
pip3 install git+https://github.com/EMKF/downwardata.git#egg=kauffman
```
We are still developing this library, so local updating will be necessary occasionally. To do this, type:
```bash
pip3 install --upgrade git+https://github.com/EMKF/downwardata.git#egg=kauffman
```

# Requirements
In order to run the `bds` or `qwi` functions, you will need a valid Census API key, either stored as an environmental variable on your computer labeled "CENSUS_KEY", or passed to the function through the "key" argument. If you do not have a census key, you can submit a request at the following URL: https://api.census.gov/data/key_signup.html.


# Library Structure
The kauffman library contains two packages:

### 1. `data`
This package contains several functions for fetching and cleaning raw data related to entrepreneurship from publicly available sources, as displayed in the following table:

Function | Source Dataset Title                   | Source Organization        |
---------| ---------------------------------------| ---------------------------|
`acs`    | American Community Survey              | U.S. Census Bureau         |
`bed`    | Business Employment Dynamics           | Bureau of Labor Statistics |
`bds`    | Business Dynamics Statistics           | U.S. Census Bureau         |
`bfs`    | Business Formation Statistics          | U.S. Census Bureau         |
`pep`    | Population Estimates Program           | U.S. Census Bureau         |
`qwi`    | Quarterly Workforce Indicators         | U.S. Census Bureau         |


### 2. `tools`
This package contains several functions for processing entrepreneurship data, grouped in the following files:
* `general_tools`: This file contains general-purpose data-transfer, crosswalk, and other types of functions that are used on and/or used to create the kauffman data.
    * `file_to_s3`
    * `file_from_s3`
    * `read_zip`
    * `aggregate_county_to_msa`
    * `geolevel_crosswalk`
    * `CBSA_crosswalk`
    * `weighted_sum`
    * `as_list`
*  `qwi_tools`: This file contains tools that relate to the qwi data. Note that there are other functions in this file not listed here that are used internally within this repository.
    * `consistent_releases`
    * `latest_releases`
    * `estimate_data_shape`
    * `missing_obs`
* `api_tools`: This file contains tools for fetching and processing data from the Census's API. Note that there are other functions in this file not listed here that are used internally within this repository.
    * `fetch_from_url`
    * `run_in_parallel`


# Feedback
Questions or comments can be directed to indicators@kauffman.org.


# Disclaimer
The content presented in this repository is presented as a courtesy to be used
only for informational purposes. The content is provided “As Is” and is not 
represented to be error free. The Kauffman Foundation makes no representation or 
warranty of any kind with respect to the content and any such representations or
warranties are hereby expressly disclaimed.