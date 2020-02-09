#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 21:11:10 2020

@author: Nikunj Maheshwari, PhD
"""

import sys
from os import path
import numpy as np
import pandas as pd
from git import Repo
REPO = Repo('.', search_parent_directories=True)


def find_rate(data=None):
    """ Find the rate of each parameter. """
    if data is None:
        print("Error. Please pass time-dependent data.")
        return None
    try:
        # Convert timestamp to seconds
        index = pd.to_datetime(data.iloc[:, 0]).values.astype(np.int64)//10**9
        data['index_col'] = index
        index = pd.to_datetime(index, unit="s")

        # get parameter values
        values = pd.Series(data.iloc[:, 1].values, index=index)

        # Find rate as difference of two consecutive values divided by
        # their date difference (in seconds)
        rate = values.diff()*(1/data['index_col'].diff().values)
        data['rate'] = rate.values

        # replace NAs with 0
        data.fillna(0, inplace=True)

    except AttributeError as error:
        print("Attribute error:" + error)
    except:
        print("Unexpected error:" + sys.exc_info()[0])
        raise


if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    FILENAMES = ["resources/paramX1wrtTime.csv",
                 "resources/paramX2wrtTime.csv", # additional data file (add your own)
                 "resources/paramX3wrtTime.csv", # additional data file (add your own)
                 "resources/paramX4wrtTime.csv" # additional data file (add your own)
                 ]

    for file in FILENAMES:
        name = file.split('/')[-1].split('wrt')
        print(name[0]) # print parameter name
        filepath = path.join(REPO.working_dir, file) # get file location
        data_file = pd.read_csv(filepath, header=0, index_col=False) # open file

        # remove rows containing NULL values
        data_file = data_file.dropna(axis=1, how="all")

        # remove leading and trailing zeros
        data_file.iloc[:, 1] = np.trim_zeros(data_file.iloc[:, 1])

        # remove NULLs as a result of trimming zeros
        data_file = data_file.dropna()

        # make sure timestamp column is the right variable type
        data_file.iloc[:, 0] = pd.to_datetime(data_file.iloc[:, 0])

        # set index of dataframe
        data_file.set_index(data_file.iloc[:, 0], inplace=True)
        find_rate(data_file) # calculate rate

        # remove 1st and 4th column. Because of inplace=True, after removing the
        # 1st column the column index updates. So to remove 4th column in
        # initial df, we set index to 2 (3rd column in the updated df)
        data_file.drop(data_file.columns[[0, 2]], axis=1, inplace=True)

        # define path for output file
        out_path = path.join(path.join(REPO.working_dir, "resources/"),
                             name[0] + "_rate.csv")

        # save original data and rate to output file
        data_file.to_csv(out_path)
