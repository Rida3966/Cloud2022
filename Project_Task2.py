# -*- coding: utf-8 -*-
"""
Created on Sat Apr  2 17:48:12 2022

@author: Rida
"""

import re
from multiprocessing import Pool
import multiprocessing as mp
from collections import Counter

def mapper(x):
    cols = x.split(',')
    #if re.match('^\\d{1,9}$', cols[1]):
    return(cols[0], cols[1])



def reducer(y):
    
    count = 0
    max_count = 0
    passenger, flights = y
    
    if passenger == passenger:
        max_count = max_count
        for i in flights:
            count += 1
            if count > max_count:
                max_count = count
    
    return max_count

def shuffler(map_out):
    data = {}
    map_out = list(filter(None, map_out))
    for k, v in map_out:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
            
    return data
    
mapper_in = []

if __name__ == "__main__":
    with open('C:\\Users\\omer_\\Downloads\\cc_work\\Passenger_data.csv', encoding='utf8') as f:
        mapper_in = f.read().splitlines()
        
    with mp.Pool(processes=mp.cpu_count()) as pool:
        mapper_out = pool.map(mapper, mapper_in, chunksize=int(len(mapper_in)/mp.cpu_count()))
        reducer_in = shuffler(mapper_out)
        reducer_out = pool.map(reducer, reducer_in.items(), chunksize=int(len(reducer_in.keys())/mp.cpu_count()))
        print(reducer_out)