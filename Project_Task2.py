

import multiprocessing as mp

""" DEFINING FUNCTIONS FOR MULTI-PROCESSING """

#########################################################

def mapper(x):
    cols = x.split(',')
    #if re.match('^\\d{1,9}$', cols[1]):
    return(cols[0], cols[1])


#########################################################
    
def combiner(y):
    
    passenger, flights = y
    count = len(flights)
    
    return (passenger,count)

#########################################################


def reducer(z):
    
    max_val = 0
    passengers = []
    for passenger in z.keys():
        if z[passenger]> max_val:
            max_val =  z[passenger]
            passengers = [passenger]
        elif z[passenger] == max_val:
            passengers.append(passenger)
            
    
    return (passengers, max_val)


#######################################################

def sorter1(map_out):
    data = {}
    map_out = set(list(filter(None, map_out)))
    for k, v in map_out:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
            
    return (data)

#########################################################


def sorter2(comb_out):
    data1 = {}
    comb_out = list(filter(None, comb_out))
    for k, v in comb_out:
       data1[k] = v
            
    return (data1)

#########################################################

""" ENTERING MAIN FILE FOR EXECUTION """
    
mapper_in = []

if __name__ == "__main__":
    with open('C:\\Users\\omer_\\Downloads\\cc_work\\Passenger_data.csv', encoding='utf8') as f:
        mapper_in = f.read().splitlines()
        
    with mp.Pool(processes=mp.cpu_count()) as pool:
        mapper_out = pool.map(mapper, mapper_in, chunksize=int(len(mapper_in)/mp.cpu_count()))
        combiner_in = sorter1(mapper_out)
        combiner_out = pool.map(combiner, combiner_in.items(), chunksize=int(len(combiner_in.keys())/mp.cpu_count()))
        reducer_in = sorter2(combiner_out)
        print(reducer(reducer_in))