# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 22:05:43 2020

@author: Nikunj
"""

from multiprocessing import Pool

# a user-defined function to multiply three numbers and return the result
def f(x,y,z):
    return x*y*z

if __name__ == '__main__':
    pool = Pool(processes=2) # start 2 pool workers
    result = pool.starmap(f, ((1, 2, 3),)) # pass function and arguments to pool workers
    print(result[0]) # print result
    pool.terminate() # kill pool workers