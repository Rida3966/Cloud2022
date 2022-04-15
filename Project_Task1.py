
import multiprocessing as mp

""" INITIALIZING FUNCTIONS FOR MULTI-PROCESSING """

###########################################################

def mapper1(m):
    
    cols = m.split(',')
    
    return (cols[0], cols[1])

############################################################
    
def mapper2(n):
    cols = n.split(',')
    
    return (cols[2], cols[1])

#############################################################

def reducer(y):
    airport, flights = y
    count = len(flights)
    
    return(airport, count)

##############################################################

"""
def shuffler(map_out1, map_out2):
    data = {}
    map_out1 = list(filter(None, map_out1))
    map_out2 = list(filter(None, map_out2))
    

    for i,j in map_out1:
        
        if i not in data:
            data[i] = [j]
        else:
            data[i].append(j)

      
        for k,v in map_out2:
            
            if (k == data.items()):
                data[k] = j[v]
                data[i] = data[k]
            else:
                 data[k].append(v)
            
    return data
 """      
##############################################################

def shuffler(airport_map, flight_map):
    codes = {}
    flights = {}
    airport_map = set(list(filter(None, airport_map)))
    flight_map = set(list(filter(None, flight_map)))
    
    
    for i,j in airport_map:
        if j not in codes:
            codes[j] = [i]
        else:
            codes[j].append(i)
        
    for k,v in flight_map:
        if codes[k][0] not in flights:
            flights[codes[k][0]]= [v]
        else:
            flights[codes[k][0]].append(v)
            
    return flights
             
#################################################################

""" ENTERING MAIN FILE FOR EXECUTION """
 
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
        reducer_out = pool.map(reducer, reducer_in.items(), chunksize=int(len(reducer_in.keys())/mp.cpu_count()))
        print(reducer_out)