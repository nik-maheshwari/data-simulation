#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 10:56:59 2020
Author: Nikunj Maheshwari

1. Use parallel computing to find the probability distribution of rate of 
   change of a parameter. 
2. Use parallel computing to find points that fits the above probability 
   distribution.
3. Read one file per parameter and generate one final file containing rate of 
   change of each parameter.

Expected input 1:
TimeStamp	        paramX	rate
06/08/2015 12:33:00	199.84	0
06/08/2015 12:33:30	199.14	-0.023333333
06/08/2015 12:34:00	199.96	0.027333333
06/08/2015 12:34:30	200.14	0.006
...

Expected input 2:
TimeStamp	        paramY	rate
06/08/2015 12:41:00	199.18	-0.047666667
06/08/2015 12:41:30	199	-0.006
06/08/2015 12:42:00	200.06	0.035333333
06/08/2015 12:42:30	199.88	-0.006
...

Expected Output:
    
paramX    paramY
-0.014455046    0.005159816
-0.010470647    -0.027910691
-0.008445245    -0.00039344
0.028559856     0.022433843
...

Running on Python 3.7.5.
"""

import time
from os import path
from multiprocessing import Pool
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde
from git import Repo
repo = Repo('.', search_parent_directories=True)

# to calculate total running time
start_time = time.time() 

if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    filenames = ["resources/paramX_rate.csv",
                ] # add more files here

    # initialise variables
    random_rate, random_values = [], []
    samples = 10000 # number of rate samples required
    rate_df = pd.DataFrame()
    pool = Pool(processes=1) # define number of parallel processes required

    # Generate time series
    for file in filenames:
        
        # split filename to get parameter name
        name = file.split('/')[-1].split('_')
        
        # print parameter name
        print(name[0])
        
        # get full file path
        filepath = path.join(repo.working_dir, file)
        
        # read file
        data_file = pd.read_csv(filepath, header=0, index_col=False)
        
        # store 'rate' column as 'data'
        data = data_file['rate'] 
        
        # use starmap() to pass a function along with its arguments to a Pool process.
        # get data points equally placed between min and max of 'data' using
        #  parallel computing
        x_grid = pool.starmap(np.linspace, ((min(data), max(data), samples),))
        
        # kernel density estimation using Gaussian kernels
        kde = pool.starmap(gaussian_kde, ((data, "scott"),))
        
        # evaluate the probability distribution of 'kde' at each point in 'x_grid'
        kdepdf = kde[0].evaluate(x_grid[0]) 
        
        # get cumulative sum
        cdf = np.cumsum(kdepdf)
        
        # divide each entry in 'cdf' by total sum
        cdf = cdf / cdf[-1]
        
        # get random samples
        values = np.random.rand(samples)
        
        # find indices in 'cdf' where elements of 'values' should be inserted 
        #  to maintain order in 'cdf'
        value_bins = pool.starmap(np.searchsorted, ((cdf, values),))
        
        # pick values of indices 'value_bins' in 'x_grid'
        random_rate = x_grid[0][tuple(value_bins)]
        
        # convert 'random_rate' to array, then transpose, then convert to list,
        #  store in a temporary dataframe
        temp_df = pd.DataFrame(np.asarray(random_rate).T.tolist(),
                               columns=[name[0]])
        
        # join temporary dataframe to 'rate_df'
        rate_df = rate_df.join(temp_df, how='outer')

    pool.terminate() # stop all parallel processes

    # write 'rate_df' to file
    rate_df.to_csv("resources/simulated_rate.csv", index=False)

    # print total running time
    print("This script took", time.time() - start_time, "sec to run")
