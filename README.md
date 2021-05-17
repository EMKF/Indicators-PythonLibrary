Python library for fetching commonly used publicly available datasets related to entrepreneurship and 
some tools used for visualization and analysis. 


# Installation
In the terminal, type
```bash
pip3 install git+https://github.com/EMKF/downwardata.git#egg=kcr
```
We are still developing this library, obviously, so local updating will be necessary occasionally. To do this, type
```bash
pip3 install --upgrade git+https://github.com/EMKF/downwardata.git#egg=kcr
```

# Feedback
Questions or comments can be directed to indicators@kauffman.org.

# Disclaimer
The content presented in this repository is presented as a courtesy to be used only for informational purposes. The 
content is provided “As Is” and is not represented to be error free. The Kauffman Foundation makes no representation or 
warranty of any kind with respect to the content and any such representations or warranties are hereby expressly 
disclaimed.


## TODO
1. library that downloads; one that pulls from database and analyzes

#### analyze
1. other viz:
    1. plot indeces of multiple series on same plot
    2. category plots
    3. think about variation across what dimension.
2. ML of last month or so...number of months could be variable
    1. Thorp-ish over/under
3. get working the choropleth functions and time series plotting
4. specify outcome variable (indicator, whatever you want to call it) and level or rate (and type of rate)
5. How would this be:
    1. data object
        * when you create object, specify list of data series you want and pull it then from database and then keep these in memory
            * but maybe this would put too much in memory.
            * but I think probably okay, since hold data in memory with pandas dataframe 
        * pull population, firm count, etc. for rate calculations
        * or maybe you choose not the raw data set but the indicator/outcome variable
        * ability to specify other data source that is in a csv file, say, and then use the tools.
        * calculates the indicators and holds in memory...this is kind of like the fit.
            * maybe something like sklearn "fit" would be best approach  
        * scratch
            * text attribute, kind of like requests.get().text...
            * maybe make it a function because I'll need to extract a lot...I don't know
    2. would need access to database
    3. attributes
        1. tail alphas
        2. growth rate
        3. statistics
            1. MAD, 
    4. functions
        1. comparisons if multiple outcomes specified
        2. plots
        3. statistical tools/diagnostics: fat tail stuff
        4. ML stuff
            1. predict where is relative to time series
            2. compare to a model, such as in naked surgeon
        5. curve estimation (like survival curve, etc.)
         
How are indicators used?
How are they interpreted?
How are they analyzed statistically?
How are they typically constructed?
    * thinking of them as rates, of some kind
    * ratio of outcomes
    * 

Indicators principles
1. What
    * get as many as I can
    * along as many dimensions as I can
    * how should they be calculated? rates, ratios, etc.
2. Analysis
    * what are the best "views" or ways of describing these?
        * get as many useful views as possible.
3. Interpretation 


#### pull
1. QA/QC pep data county and msa data
2. cleanup pep_helpers
    1. get rid of unused functions
    2. refactor redundant code
2. etl, cw function, groupby too, not just merge
2. cross walk tool in tools module.
3. PEP state level functions...cleanup 
4. thousands parameter in read_excel and probably read_csv
5. some convetion for the dics in constants and updated in the scripts