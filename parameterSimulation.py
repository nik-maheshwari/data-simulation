#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 13:27:34 2020
Author: Nikunj Maheshwari

Simulate data having similar correlation as original dataset using parallel 
computing in Python

"""

import time
from multiprocessing import Pool
import pandas as pd
import numpy as np
from git import Repo
repo = Repo('.', search_parent_directories=True)
start_time = time.time()


def simulate_corr(temp):
    """
    Simulate original dataset, using rates and correlation. Pick a random
    column (X), simulate other column values based on rate of change of X and
    correlation between X and other columns. Repeat for samples count.
    """
    global pos, neg, lower, upper, sdev, rate, orig_df, freq, samples

    data = orig_df
    colRange = len(data.columns) # number of columns
    start_vals = data.mean() # get mean
    data_corr = data.corr() # get correlation between all columns
    colms = data.iloc[:, 0:colRange].columns.values.tolist() # list of columns
    temp_df = pd.DataFrame(columns=colms) # empty df intialised with columns

    # simulates samples of rows, each containing simulated values for each column
    for i in range(samples):
        
        # print the progress of the simulation
        if i % 1000 == 0:
            print("Simulated", i, "of", samples, " parameters in chunk:", temp)
        index = int(np.random.randint(colRange, size=1)) # random index
        randomCol = colms[index] # get the random column name (say, paramX)
        end_total = list()
        newrate = 0

        # simulate each columns with respect to the selected paramX, trying to
        #  keep correlation intact
        for col in colms:
            start = start_vals[col] # start with the mean of the column
            corr = data_corr[randomCol][col] # get correlation randomColue between column and paramX
            delta = rate[col].sample(1).values # pick a random rate value for column

            #  First multiply the ratio of standard deviation (sdev) of column
            #   and paramX with 'corr' (this gives the new correlation).
            #  Then multiply the rate (delta) with frequency to get the new value.
            #  But this new value needs to be normalised to reflect the
            #   correlation between the columns, so multiply the new value with
            #   the new correlation.
            nextValue = start + (sdev[col]/sdev[randomCol])*corr*delta*freq

            #  New value may exceed the range of the column in the original dataset.
            #   If so, get a different value of rate (pick positive rate if new
            #   value is less than min, and vice versa) and calculate the new
            #   value. Repeat this until the new value falls within the range.
            #  This is a time-consuming but necessary step.
            while (nextValue < lower[col]) | (nextValue > upper[col]):
                if nextValue < lower[col]:
                    newrate = pos[col].sample(1).values
                else:
                    newrate = neg[col].sample(1).values
                delta = newrate
                nextValue = start + (sdev[col]/sdev[randomCol])*corr*delta*freq

            # append the new value to end_total
            end_total.append(nextValue)

            # simulate the next value taking nextValue as start
            start_vals[col] = nextValue

        # when the row has been fully simulated for all columns, append to df
        temp_df = temp_df.append(pd.DataFrame(np.asarray(end_total).T, columns=colms),
                                 ignore_index=True)

    # round up all values to 3 decimals
    temp_df = temp_df.round(3)
    return temp_df # return simulated df


if __name__ == '__main__':
    rate = pd.read_csv("../resources/simulated_rate.csv", header=0, index_col=False)
    print("Read rate file\n")
    concat_df, backup_df = pd.DataFrame(), pd.DataFrame()
    orig_df = pd.read_csv("../resources/original.csv", index_col=False)
    print("Read original df\n")

    sdev = orig_df.std()
    
    # manually input the upper and lower limit of each parameter. These limits
    # should come from the domain knowledge. For ex., a subject matter expert 
    # can say that the oxygen level in the tank cannot exceed 72%, so that 
    # sets the upper limit for that parameter.
    upper = {"paramX": 67.22,
             "paramY": 53.68,
            }
    lower = {"paramX": 31.9,
             "paramY": 41.76,
            }

    # store positive and negative simulated rates of all columns
    pos, neg = {}, {}
    for value in rate.columns.values:
        pos[value] = rate[rate[value] > 0][value]
        neg[value] = rate[rate[value] < 0][value]

    runs = list(range(1, 1126))  # total number of runs = 1125 (75 x 15)
    start_time = time.time()
    print("Simulating time dependent params....\n")
    pool = Pool(75) # create 75 worker processes
    samples = 40320 # each run will create 40320 samples (rows)

    # each row will increment by 30 seconds. Also used to calculate the next value
    freq = 30

    # send 15 runs to each worker process. Start parallel processes, and collect
    #  results in a single list
    sim_df = pool.map(simulate_corr, runs, chunksize=15)

    # kill processes after runs are completed
    pool.terminate()

    # total time taken (~40 days)
    print("Pooling took", time.time() - start_time, "sec to run")
    concat_df = pd.concat(sim_df) # convert the list to df
    concat_df.reset_index(drop=True, inplace=True) # remove the row index
    print("Total number of samples = ", len(concat_df))

    samples_ind = len(concat_df) + 1

    # generate time parameters. Choose any starting date and the frequency you
    # wish to simulate the time column.
    time_range = pd.DataFrame(pd.date_range('1/1/2000 15:00:00', periods=samples_ind,
                                            freq='30S'), columns=["Time"])

    concat_df["Time"] = time_range["Time"] # add new column called "Time"
    concat_df.set_index("Time", inplace=True) # convert "Time" to index
    
    # save the df as a CSV file. This could be in GB, so make sure you have
    # enough hard drive space in the location.
    concat_df.to_csv("../resources/simulation_45million_all.csv")
    

### END ###