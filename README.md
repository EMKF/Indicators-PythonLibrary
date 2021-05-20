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
1. QA/QC pep data county and msa data
2. etl, cw function, groupby too, not just merge
2. time series plotting and choropleth functions
2. * cross walk tool in tools module.
3. PEP state level functions...cleanup 
4. thousands parameter in read_excel and probably read_csv
5. some convetion for the dics in constants and updated in the scripts