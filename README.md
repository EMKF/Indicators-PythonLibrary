# Overview
`kauffman` is a Python library for fetching and cleaning commonly used publicly available datasets related to entrepreneurship.

# Installation
In the terminal, type
```bash
pip3 install git+https://github.com/EMKF/downwardata.git#egg=kcr
```
We are still developing this library, obviously, so local updating will be necessary occasionally. To do this, type
```bash
pip3 install --upgrade git+https://github.com/EMKF/downwardata.git#egg=kcr
```

# Requirements
In order to run the `bds` or `qwi` functions, you will need a valid Census API key, either stored as an variable on your computer labeled "CENSUS_KEY", or passed to the function through the "key" argument. If you do not have a census key, you can submit a request at the following URL: https://api.census.gov/data/key_signup.html.


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
`shed`   | Survey of Household Economics and Decisionmaking | Federal Reserve  |


### 2. `tools`
This package contains several functions for processing entrepreneurship data, within the following categories:
* File-processing functions:
    * `file_to_s3`
    * `file_from_s3`
    * `read_zip`
* Crosswalk functions:
    * `load_CBSA_cw`
    * `county_msa_cross_walk`
    * `state_msa_cross_walk`
    * `fips_state_cross_walk`
* Miscellaneous functions:
    * `weighted_sum`
    * `race_ethnicity_categories_create`

# Feedback
Questions or comments can be directed to indicators@kauffman.org.


# Disclaimer
The content presented in this repository is presented as a courtesy to be used
only for informational purposes. The content is provided “As Is” and is not 
represented to be error free. The Kauffman Foundation makes no representation or 
warranty of any kind with respect to the content and any such representations or
warranties are hereby expressly disclaimed.