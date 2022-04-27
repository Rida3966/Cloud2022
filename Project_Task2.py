
# Importing essential libraries
import multiprocessing as mp
import pandas as pd

#########################################################

""" DEFINING FUNCTIONS FOR MULTI-PROCESSING """

#########################################################

""" MAPPER FUNCTION"""

def mapper(x):
    
    # Comma delimiter template
    cols = x.split(',')
        
    return(cols[0], cols[1])


#########################################################

""" REDUCER FUNCTION """

def reducer(z):
    
    # Calculating passenger/passengers having maximum number of flights
    passengers = [(key, value) for key,value in z.items() if value == max(z.values())]
    
    return (passengers)

#########################################################

""" COMBINER FUNCTION """

def combiner(y):
    
    passenger, flights = y
    
    # Counting number of flights for each passenger
    count = len(flights)
    
    return (passenger,count)


#######################################################

""" SORTER FOR MAPPER OUTPUT """

def sorter1(map_out):
    data = {}
    
    # Filtering duplicate pairs and Null values from mapper list
    map_out = set(list(filter(None, map_out)))
    
    # Sorting mapper results for Combiner function
    for k, v in map_out:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
            
    return (data)

#########################################################

""" SORTER FOR COMBINER OUTPUT """

def sorter2(comb_out):
    
    data = {}
    
    # Filtering any Null values from combiner list
    comb_out = list(filter(None, comb_out))
    
    
    # Transforming list of tuples to Dictionary of (key:value) pairs for reducer job
    for k, v in comb_out:
       data[k] = v
            
    return (data)

#########################################################

""" INPUT FILE FORMAT """

def readInputFile(a):
    
    map_in=[]
    
    with open(a, encoding='utf8') as f:
        
        # Line split to iterate through each row 
        map_in = f.read().splitlines()
        
        # Removing Null values from input records
        map_in= list(filter(None, map_in))
        
    return (map_in)

#########################################################

""" MAIN FUNCTION """

def main():
    
    # Add input file as mapper input
    mapper_in = readInputFile('C:\\Users\\omer_\\Downloads\\cc_work\\Passenger_data.csv')  
    
    # Pooling function to carry our multi-processing
    with mp.Pool(processes=mp.cpu_count()) as pool:
        
        # Mapper job
        mapper_out = pool.map(mapper, mapper_in, chunksize=int(len(mapper_in)/mp.cpu_count()))
        
        # Combiner Job
        combiner_in = sorter1(mapper_out)
        combiner_out = pool.map(combiner, combiner_in.items(), chunksize=int(len(combiner_in.keys())/mp.cpu_count()))
        
        #Reducer Job
        reducer_in = sorter2(combiner_out)
        reducer_out = reducer(reducer_in)
        
        # Displaying output in tabular form
        headings = ["Passenger id", "Maximum Flights"]
        Table = pd.DataFrame(columns = headings, data = reducer_out)
        print('"Passengers having maximum number of flights"\n\n',Table)
        
        # Output to csv file
        Table.to_csv('Task2_Results.csv', index = False, header = True)
        

############################################################

""" ENTERING MAIN FILE FOR EXECUTION """

if __name__ == "__main__":
    main()
   
##############################################################   