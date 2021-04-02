# TODO
1. what else need's to be in here to make it a library? Like an __init__.py file? How are these used, typically?
2. create a kauffman_tools library for things like the time series plotting and choropleth functions
    1. probably best in a different repository
3. docstrings
4. requirements.txt file


# Installation
In the terminal, type
```bash
pip3 install git+https://github.com/EMKF/downwardata.git#egg=kcr
```
We are still developing this library, obviously, so local updating will be necessary occasionally. To do this, type
```bash
pip3 install --upgrade git+https://github.com/EMKF/downwardata.git#egg=kcr
```

Plotly was a bit of a beast to install. To write to file needed to install orca at https://github.com/plotly/orca


# Documentation and Examples
I added draft docstrings in the ```get_data``` functions for bds, bds, lfs, and pep modules.
But it's still pretty weak. We'll have to add more documentation at some point. 
The following are a few examples using the library: 
* BDS: https://github.com/EMKF/narrative/blob/master/bds/over_time.py
* LFS (labor force statistics): https://github.com/EMKF/narrative/blob/master/labor_force_statistics/self_employment_over_time.py

```python
from kauffman import   lfs
from kauffman import bds_helpers, bfs_helpers, pep_helpers
```
## BFS
```python
bfs.get_data(['BF_DUR4Q', 'BF_DUR8Q', 'BA_BA'], 'state', 2004, annualize=True)
```
This returns a pandas dataframe with the following characteristics:
* it has five columns: Period, region, BF_DUR4Q, BF_DUR8Q, and BA_BA
* the level of observation is state
* the first possible year in the data set is 2004 (ends up being 2005 because of annualization)
* the last year is the most current year available for each BFS variable
* the data are annualized, which means a year is dropped completely if all four quarters of data are not in the dataset. 

```python
bfs.get_data(['BF_DUR4Q', 'BA_BA', 'BF_BF8Q'], 'us', 2010, end_year=2012, annualize=False)
```
This returns a pandas dataframe with the following characteristics:
* it has five columns: Period, region, BF_DUR4Q, BA_BA, and BF_BF8Q
* the level of observation is U.S.
* the first year in the dataset is 2010
* the last year is 2012
* the data are quarterly (i.e., not annualized) 

## PEP
```python
pep.get_data('us', 2011, 2018)
```
This returns a pandas dataframe with the following characteristics:
* it has three columns: population, year, region
* the level of observation is U.S.
* the first year in the dataset is 2011
* the last year is 2018
* data are annual
Replacing the keyword 'us' with 'state' will give state level data. 


### PublicDataHelpers Class pandas extension
If ```df``` is a dataframe with certain attributes (using the bds and lfs modules create dataframes that qualify), 
plotting the data is as simple as  
```python
df.pub.plot()
```

```python
import kauffman.constants as c
import kauffman.kauffman_tools.public_data_helpers as p

df = p.raw_kese_formatter(c.filenamer('../scratch/Kauffman Indicators Data State 1996_2019_v3.xlsx'), c.filenamer('../scratch/Kauffman Indicators Data National 1996_2019_v3.xlsx'))
df_out = df.pub.download_to_alley_formatter(['type', 'category'], 'rne')

```
Changing the parameters (see examples and documentation) makes the plots nicer.


# TODO
* structure:
    * data, tools subdirectories
* remove main blocks
* have test scripts
* 


## panel to alley

## plotter
* how to wipe out figure if I don't want to show it.
* fit the trend and then filter to only the relevant years
* It would be nice to be able to combine time series onto the same plot

## done
* bfs
* pep (national, state)

## todo
* pep
    * individual states or subset of all states
    * county
* bfs
    * individual states or subset of all states
* ase
    * finish constructing the dictionary named ase_url_dict from constants.py (at the bottom of the script); update the docstring in ase.py accordingly.
    * functions that clean the data and return cleaned (but still as raw as possible) data
* data_manager
    * generalize the zip_to_dataframe function as specified in the doc string
   
# Development documentation
* https://uoftcoders.github.io/studyGroup/lessons/python/packages/lesson/