from kauffman.eship_data_sources import bfs, bds

__all__ = ['bfs', 'bds']

# todo:
#   1. consistent api across datasources in, essentially the get_data function from each of the datasource-specific files put into the eship_data_sources.py file
#       1. variables
#           1. list
#           2. must be provided by the user
#       2. region
#           1. string or list
#           2. 'all', 'us', 'state', 'county' (when applicable) or a list of fips codes of specific regions
#       3. the output dataframe from each function should consist of the following variables in the following order
#           1. 'fips': fips code of region
#           2. 'region': region name
#           3. 'time': datetime formatted variable representing the observation time
#           4. specified variable list
#       4. test that they work using test_basic.py
#           1. you should be able to run this as is, without any wizardry
#   2. source specific function parameters
#       1. aggregation time frame to quarter or year
#       2. seasonal data or not
#       3. on=[]
#           1. variables to condition the data on
#   3. doc strings
#       1. start with functions in eship_data_sources
#           1. describe data source, link to documentation, years available, level of observation, list of variables
#       2. helper functions probably don't need docstrings, at least not yet.
#   4. helpers subdirectory
#       1. after moving the get_data functions, delete.
#       2. rename file from datasource.py to datasource_helpers.py
#       3. move these helpers files to a new subdirectory named "helpers"
#   5. cleanup the constants.py file
#       1. won't be able to do this until at least the qwi function is integrated into eship_data_sources.py and tested
#   6. can we move data_manager.py into the kauffman_tools subdirectory in the scratch directory?
#   7. get it ready for publication on pypi
#       1. https://medium.com/@joel.barmettler/how-to-upload-your-python-package-to-pypi-65edc5fe9c56
#       2. as part of this, rename the repo to eship_data
#       3. update readme.md file