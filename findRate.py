#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 21:11:10 2020

@author: Nikunj Maheshwari, PhD

This script finds the rate of change per second of a time-series parameter.
At the end of the script, an output file is created with the original data and
an additional column called 'rate' containing rate of change per second.

Example:

Input -
TimeStamp,paramX
06/08/2015 12:33:00,199.84
06/08/2015 12:33:30,199.14
06/08/2015 12:34:00,199.96
06/08/2015 12:34:30,200.14

Output -
TimeStamp,paramX,rate
2015-06-08 12:33:00,199.84,0.0
2015-06-08 12:33:30,199.14,-0.02333
2015-06-08 12:34:00,199.96,0.02733
2015-06-08 12:34:30,200.14,0.006
"""

import sys
from os import path
import numpy as np
import pandas as pd
from git import Repo

# to get the full path of the input file
REPO = Repo('.', search_parent_directories=True)


""" 
A function to find the rate of a time-dependent parameter. 

Input - Dataframe with 2 columns (TimeStamp and Paramater values)
Ex:
TimeStamp  paramX
2015-06-08 12:33:00	199.84
2015-06-08 12:33:30	199.14
2015-06-08 12:34:00	199.96

Output - Dataframe with additional columns called 'index_col' and 'rate'
Ex:
TimeStamp  paramX  index_col  rate
2015-06-08 12:33:00	199.84	1433766780	0.0
2015-06-08 12:33:30	199.14	1433766810	-0.02333
2015-06-08 12:34:00	199.96	1433766840	0.02733

"""

def find_rate(data=None):
    
    if data is None:
        print("Error. Please pass time-dependent data.")
        return None
    try:
        # Convert TimeStamp (which is in nanoseconds) to seconds
        index = pd.to_datetime(data.iloc[:, 0]).values.astype(np.int64)//10**9
        
        # create a new column with index values
        data['index_col'] = index

        # get parameter values and convert to series
        values = pd.Series(data.iloc[:, 1].values, index=index)

        # Find rate as difference of two consecutive values divided by
        # their time difference (in seconds)
        rate = values.diff()/(data['index_col'].diff().values)
        
        # create a new column 'rate' with the rate values
        data['rate'] = rate.values

        # replace NAs with 0
        data.fillna(0, inplace=True)
        
        # round up the 'rate' column to 5 decimals
        data['rate'] = data['rate'].round(5)

    except AttributeError as error:
        print("Attribute error:" + error)
    except:
        print("Unexpected error:" + sys.exc_info()[0])
        raise


if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    FILENAMES = ["resources/paramX1wrtTime.csv",
                 #"resources/paramX2wrtTime.csv", # additional data file (add your own)
                 #"resources/paramX3wrtTime.csv", # additional data file (add your own)
                 #"resources/paramX4wrtTime.csv" # additional data file (add your own)
                 ]

    for file in FILENAMES:
        # split the file path to get the parameter name
        name = file.split('/')[-1].split('wrt')
        
        # print parameter name
        print(name[0])
        
        # get full file path
        filepath = path.join(REPO.working_dir, file)
        
        # open the file
        data_file = pd.read_csv(filepath, header=0, index_col=False) 
        
        # remove rows containing NULL values
        data_file = data_file.dropna(axis=1, how="all")
        
        # remove leading and trailing zeros in the 2nd column
        data_file.iloc[:, 1] = np.trim_zeros(data_file.iloc[:, 1])
        
        # remove NULLs as a result of trimming zeros
        data_file = data_file.dropna()
        
        # get column data types. TimeStamp column is of type 'object'
        data_file.dtypes
        
        # convert TimeStamp column to the right data type
        data_file.iloc[:, 0] = pd.to_datetime(data_file.iloc[:, 0])
        
        # successfully converted TimeStamp to type 'datetime'
        data_file.dtypes
        
        # set the 1st column (TimeStamp) as the index of dataframe
        data_file.set_index(data_file.iloc[:, 0], inplace=True)
        
        # find_rate is our custom-made function which takes a single dataframe
        # as an argument. The function adds additional columns to our original
        # dataframe (data_file). In Python, parameters are passed by reference to a 
        # function, hence any changes done in the function appears in the original 
        # parameter. For this reason, additional columns created in the function
        # 'find_rate' appears in the dataframe that was passed to it.
        find_rate(data_file)
        
        # remove 1st and 4th column. Because of inplace=True, after removing the
        # 1st column, the column index updates. So to remove 4th column in
        # initial df, we set index to 2 (3rd column in the updated df)
        data_file.drop(data_file.columns[[0, 2]], axis=1, inplace=True)
        
        # define path for output file. Take the original parameter name and append
        # '_rate' to label the file as the one containing the calculated rate column
        out_path = path.join(path.join(REPO.working_dir, "resources/"),
                             name[0] + "_rate.csv")
        
        # save original data and rate to output file
        data_file.to_csv(out_path)
        
