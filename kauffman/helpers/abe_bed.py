import pandas as pd
import requests
import numpy as np

#These are now in a format that the bed data create function type should work


def _data_lines(table = 1, firm_size = 1):

    url = f'https://www.bls.gov/web/cewbd/f.0{firm_size}.table{table}_d.txt'
    lines = requests.get(url).text.split('\n')
    return lines
# 1 and 2 should include the thousands code and comma removal, 3 and 4 tables should not.

def table_1(lines): #works for 1 and 2
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


    df = pd.DataFrame(
            row,
            columns=['time', 'quarter', 'net_change', 'total_gains', 'gross_job_gains_expanding_firms', 'gross_job_gains_opening_firms', 'total_losses', 'gross_job_losses_contracting_firms', 'gross_job_losses_closing_firms']
        ) #.\
        # pipe(_format_covars1)

    df.replace(',', '', regex=True, inplace=True)
    df.iloc[:, 2:9] = df.iloc[:, 2:9].apply(pd.to_numeric)
    df.iloc[:, 2:9] = df.iloc[:, 2:9] * 1000

    return df


def table_3(lines): #works for 3 and 4
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


    df = pd.DataFrame(
            row,
            columns=['time', 'quarter', 'net_change', 'total_gains', 'gross_job_gains_expanding_firms', 'gross_job_gains_opening_firms', 'total_losses', 'gross_job_losses_contracting_firms', 'gross_job_losses_closing_firms']
        ) #.\
        # pipe(_format_covars1)

    df.iloc[:, 2:9] = df.iloc[:, 2:9].apply(pd.to_numeric)


    return df

#columns are in the right order. I don't think fips and region codes apply for these tables, could be wrong.



def abe_data_create(table, firm_size): #todo test this

    """
    Private Sector Firm-Level Job Gains and Losses

    table:
        1: Seasonally Adjusted, 2: Not Seasonally Adjusted, 3: As % Employment Seasonally Adjusted, 4: As % Employment Not Seasonally Adjusted
    firm size: (number of employees)
        1: 1-4 , 2: 5-9 , 3: 10-19 , 4: 20-49 , 5: 50-99, 6: 100-249 , 7: 250-499 , 8: 500-999 , 9: >1000

    """
    print(f'Fetching table {table} for firm size {firm_size}')

    if table in range(1, 3):
        df = table_1(_data_lines(table, firm_size))
    if table in range(3, 5):
        df = table_3(_data_lines(table, firm_size))
    return df

#test. the code works
abe_data_create(3,9)