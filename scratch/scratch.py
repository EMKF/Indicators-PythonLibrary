# imports
import numpy as np
import pandas as pd

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)



# functions
def adder(x, y):
    print(x + y)

def common_data_commands():
    # print("Hey, how's it going?")

#     read in a datafile
    df = pd.read_csv('/Users/thowe/Downloads/bds_e_sz_release.csv')

    print('Information of the dataset...')
    print(df.info())

    print('\nDescription of each variable...')
    print(df.describe())

    print('\nFirst five lines of the data...')
    print(df.head())

    print('\nPrint all of data...')
    print(df)

# main block
if __name__ == '__main__':
    # adder(1, 3)
    common_data_commands()
