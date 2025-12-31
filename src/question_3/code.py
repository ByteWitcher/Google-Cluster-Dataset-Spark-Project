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
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(',')]

    # Skip rows with missing CPU value
    if splitLine[4] == '':
        return None

    return (
        int(splitLine[0]),
        int(splitLine[1]),
        int(splitLine[2]),
        float(splitLine[4])
    )

# Given a list of events for a machine, compute the total lost CPU time due to REMOVE events
def compute_lost_cpu_time(events):
    lost = 0.0
    last_remove_time = None
    cpu_capacity = events[0][2]

    for time, event, cpu in events:
        if event == 1:  # REMOVE
            cpu_capacity = cpu
            last_remove_time = time
        elif event == 0 and last_remove_time is not None:  # ADD after REMOVE
            lost += cpu_capacity * (time - last_remove_time)
            last_remove_time = None

    return (cpu_capacity,lost)

#### Driver program

# Start spark 
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

# Compute lost CPU time for each machine
lost_cpu_time_by_machine = sorted_events_by_machine.mapValues(compute_lost_cpu_time)

# Total lost CPU time across all machines
total_lost_cpu_time = lost_cpu_time_by_machine.map(lambda x: x[1][1]).sum()

# Compute percentage of lost CPU time by CPU capacity
lost_cpu_time_by_cpu_capacity = lost_cpu_time_by_machine.map(lambda x: x[1]).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0],(x[1]/total_lost_cpu_time)*100)).sortByKey().collect()

cpu_values = [x[0] for x in lost_cpu_time_by_cpu_capacity]
lost_percentages = [x[1] for x in lost_cpu_time_by_cpu_capacity]

# Create directory if it does not exist
os.makedirs("./src/question_3/plots", exist_ok=True)

# Plot results 
x_pos = range(len(cpu_values))

plt.figure(figsize=(8, 5))
plt.bar(x_pos, lost_percentages, width=0.6)

plt.xticks(x_pos, cpu_values)
plt.xlabel("CPU capacity (normalized)")
plt.ylabel("Lost CPU Time (%)")
plt.title("Percentage of Lost CPU Time by CPU Capacity")

plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig("./src/question_3/plots/lost_cpu_time_percentage.png", dpi=300)