# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 20:08:11 2022

@author: rida
"""

import re
from multiprocessing import Pool
import multiprocessing as mp

def mapper1(x):
    
    cols = x.split(',')
    
    return (cols[0], cols[1])
    


def mapper2(m):
    cols = m.split(',')
    
    return (cols[2], cols[1])
"""

def reducer(y):
    
    code, flights = y
    
    return (code, len(flights))

"""
def reducer(y):
    count = []
    passenger, flights = y
    count = len(flights)
    #max_count = max(count)

    return (count)

    #max_count = max(range(count))
    #return (count)
    #return (passenger, count)
    #return (count)

def shuffler(map_out1, map_out2):
    data = {}
    map_out1 = list(filter(None, map_out1))
    map_out2 = list(filter(None, map_out2))
    
    for i,j in map_out1:
        if i not in data:
            data[i] = [j]
        else:
            data[i].append(j)
                

    for k, v in map_out2:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
         
    return data

 
mapper_in = []

if __name__ == "__main__":
    with open('C:\\Users\\omer_\\Downloads\\cc_work\\Top30_airports_LatLong(1).csv', encoding='utf8') as f1:
         a = f1.read().splitlines()
         mapper_in1= list(filter(None, a))
         
    with open('C:\\Users\\omer_\\Downloads\\cc_work\\Passenger_data.csv', encoding='utf8') as f2:
        b = f2.read().splitlines()
        mapper_in2= list(filter(None, b))
        
    with mp.Pool(processes=mp.cpu_count()) as pool:
        mapper_out1 = pool.map(mapper1, mapper_in1, chunksize=int(len(mapper_in1)/mp.cpu_count()))
        mapper_out2 = pool.map(mapper2, mapper_in2, chunksize=int(len(mapper_in2)/mp.cpu_count()))
        reducer_in = shuffler(mapper_out1, mapper_out2)
        #reducer_out = pool.map(reducer, reducer_in.items(), chunksize=int(len(reducer_in.keys())/mp.cpu_count()))
        print(reducer_in)