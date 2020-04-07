# Installation
In the terminal, type
```bash
pip3 install git+https://github.com/EMKF/downwardata.git#egg=kcr
```
We are still developing this library, obviously, so local updating will be necessary occasionally. To do this, type
```bash
pip3 install --upgrade git+https://github.com/EMKF/downwardata.git#egg=kcr
```

# Documentation and Examples
I added draft docstrings in the ```get_data``` functions for bds, bds, lfs, and pep modules.
But it's still pretty weak. We'll have to add more documentation at some point. 
The following are a few examples using the library: 
* BDS: https://github.com/EMKF/narrative/blob/master/bds/over_time.py
* LFS (labor force statistics): https://github.com/EMKF/narrative/blob/master/labor_force_statistics/self_employment_over_time.py

```python
from kauffman_data import bfs, pep, lfs, bds
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
Changing the parameters (see examples and documentation) makes the plots nicer.


# TODO
## done
* bfs
* pep (national, state)

## todo
* pep
    * individual states or subset of all states
    * county
* bfs
    * individual states or subset of all states
    
   
# Development documentation
* https://uoftcoders.github.io/studyGroup/lessons/python/packages/lesson/