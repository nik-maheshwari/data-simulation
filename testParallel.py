# -*- coding: utf-8 -*-
"""
Created on Wed Mar 25 22:05:43 2020

@author: Nikunj
"""

from multiprocessing import Pool

def f(x,y,z):
    return x*y*z

if __name__ == '__main__':
    pool = Pool(processes=1)
    t = pool.starmap(f, ((1, 2, 3),))
    print(t[0])
    pool.terminate()