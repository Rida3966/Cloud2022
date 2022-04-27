
# Importing essential libraries
import multiprocessing as mp
import pandas as pd

##########################################################

""" INITIALIZING FUNCTIONS FOR MULTI-PROCESSING """

###########################################################

""" MAPPER FUNCTION FOR FIRST MAP JOB (i.e. Mapping Airport names with FAA code) """

def mapper1(m1):
    
    # Comma delimiter template 
    cols = m1.split(',')
    
    return (cols[0], cols[1])

############################################################

""" MAPPER FUNCTION FOR SECOND MAP JOB (i.e. Mapping FAA code of departure airport with Flight id) """
    
def mapper2(m2):
    
    # Comma delimiter template 
    cols = m2.split(',')
    
    return (cols[2], cols[1])

#############################################################

""" REDUCER FUNCTION """

def reducer(z):
    airport, flights = z
    
    # Counting number of flights for each airport
    count = len(flights)
    
    return(airport, count)


##############################################################

""" SHUFFLER FUNCTION (To aggregarte/merge results of both mappers) """

def shuffler(x, y):

    codes = {}
    flights = {}
    
    # Removing duplicate sets in file
    airport_map = set(list(filter(None, x)))
    flight_map = set(list(filter(None, y)))
    
    # Iterating through first list
    for i,j in airport_map:
        if j not in codes:
            codes[j] = [i]
        else:
            codes[j].append(i)
            
    # Iterating on second list and grouping results by FAA codes   
    for k,v in flight_map:
        if codes[k][0] not in flights:
            flights[codes[k][0]]= [v]
        else:
            flights[codes[k][0]].append(v)
            
    return flights

#################################################################

""" INPUT FILE FORMAT """

def readInputFile(a):
    
    map_in=[]
    
    with open(a, encoding='utf8') as f:
        
        # Line split to iterate through each row 
        map_in = f.read().splitlines()
        
        # Removing Null values from input records
        map_in= list(filter(None, map_in))
        
    return (map_in)
             
#################################################################

""" MAIN FUNCTION """

def main():
     map_in1 = readInputFile('C:\\Users\\omer_\\Downloads\\cc_work\\Top30_airports_LatLong(1).csv')
     map_in2 = readInputFile('C:\\Users\\omer_\\Downloads\\cc_work\\Passenger_data.csv')
        
    # Multi-processing (Thread corresponding functions)    
     with mp.Pool(processes=mp.cpu_count()) as pool:
        
        # Mapper Jobs
        map_out1 = pool.map(mapper1, map_in1, chunksize=int(len(map_in1)/mp.cpu_count()))
        map_out2 = pool.map(mapper2, map_in2, chunksize=int(len(map_in2)/mp.cpu_count()))
        
        # Shuffler Job
        reducer_in = shuffler(map_out1, map_out2)
        
        # Reducer Job
        reducer_out = pool.map(reducer, reducer_in.items(), chunksize=int(len(reducer_in.keys())/mp.cpu_count()))
        
        # Displaying Output in Table format
        headings = ["Airport Name", "No. of Flights"]
        Table = pd.DataFrame(columns = headings, data = reducer_out)
        print('"Total flights from each airport"\n\n',Table)
        
        # Output to csv file
        Table.to_csv('Task1_Results.csv', index = False, header = True)
  
#################################################################

""" ENTERING MAIN FILE FOR EXECUTION """

if __name__ == "__main__":
    
    main()
     
#################################################################