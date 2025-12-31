import os
import matplotlib.pyplot as plt
from pyspark import SparkContext

# Machine Events
# 0,time,INTEGER,YES
# 1,machine ID,INTEGER,YES
# 2,event type,INTEGER,YES : ADD (0) | REMOVE (1) | UPDATE (2)
# 3,platform ID,STRING_HASH,NO
# 4,CPUs,FLOAT,NO
# 5,Memory,FLOAT,NO

# For each line of the input file, return a tuple (time, machine ID, event type, CPUs)
def parseLine(line):
    splitLine = line.split(',')

    # Skip rows with missing CPU value
    if len(splitLine) < 6 or splitLine[4] == '':
        return None
    
    return (
        int(splitLine[0]),
        int(splitLine[1]),
        int(splitLine[2]),
        float(splitLine[4])
    )

def compute_lost_cpu_time(events):
    lost = 0.0
    last_remove_time = None
    cpu_capacity = None

    for time, event, cpu in events:
        if event == 1:  # REMOVE
            last_remove_time = time
            cpu_capacity = cpu
        elif event == 0 and last_remove_time is not None:  # ADD after REMOVE
            lost += cpu_capacity * (time - last_remove_time)
            last_remove_time = None

    return lost

#### Driver program

# Start spark with 1 worker thread
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
events = sc.textFile("./data/machine_events/part-00000-of-00001.csv")

# Parse each line into a tuple (time, machine ID, event type, CPUs)
parsed_events = events.map(parseLine).filter(lambda x: x is not None)

# Group events by machine ID
events_by_machine = parsed_events.map(lambda x: (x[1], (x[0], x[2], x[3]))).groupByKey()

# Sort events for each machine ID by time
sorted_events_by_machine = events_by_machine.mapValues(lambda evts: sorted(evts, key=lambda x: x[0]))
