import pandas as pd
import requests
import numpy as np

#These are now in a format that the bed data create function type should work


def _data_lines(table = 1):

    url = 'https://www.bls.gov/web/cewbd/f.01.table1_d.txt'
    # url = 'https://www.bls.gov/web/cewbd/f.01.table{table}_d.txt'
    lines = requests.get(url).text.split('\n')


def table_1(lines):
    lines = lines[12:-2] #remove explanation lines

    row = []
    year = 1992
    for line in lines:

        line_list = line.split()
        if line_list: #gets rid of white space by splitting on it

            if line_list[0].isnumeric(): #Checks if the line starts with a year
                row.append(line_list) #adds the row to the list that will create dataframe
                year = line_list[0] #sets the year to the most recent year line
            if line_list[0] in ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']:
                row.append([year] + line_list) #if the first element is a month, inputs the most recent year, concatenates the list, and appends

    row[2:] = row[2:]*1000

    df = pd.DataFrame(
            row,
            columns=['time', 'quarter', 'net_change', 'total_gains', 'gross_job_gains_expanding_firms', 'gross_job_gains_opening_firms', 'total_losses', 'gross_job_losses_contracting_firms', 'gross_job_losses_closing_firms']
        ) #.\
        # pipe(_format_covars1)
    df = df.replace(',', '', regex=True, inplace=True)
    df.iloc[:, 2:9] = df.iloc[:, 2:9].apply(pd.to_numeric)
    df.iloc[:, 2:9] = df.iloc[:, 2:9] * 1000

    return df

#columns are in the right order. I don't think fips and region codes apply for this table, could be wrong.

# i need to remove the commas from the numbers, convert them all to integers, then the multiplication code will work. Check if any of travis' functions already to that. SO CLOSE!