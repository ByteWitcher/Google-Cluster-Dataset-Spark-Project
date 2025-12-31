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

#### Driver program

# Start spark 
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
events = sc.textFile("./data/machine_events/part-00000-of-00001.csv")

# Parse each line into a tuple (time, machine ID, event type, CPUs)
parsed_events = events.map(parseLine).filter(lambda x: x is not None)

# Filter out REMOVE events
filtered_events = parsed_events.filter(lambda x: x[2] != 1)

# Transfrom to key-value pairs of the form (machine ID, (time, CPUs))
event_key_value_pairs = filtered_events.map(lambda x: (x[1], (x[0], x[3])) )

# For each machine ID, get the event with the latest time
most_recent_machine_events = event_key_value_pairs.reduceByKey(lambda x,y: x if x[0] > y[0] else y)

# Extract the CPU values from the results
cpus = most_recent_machine_events.map(lambda x: x[1][1])

# Compute the distribution of CPU values
cpu_distribution = cpus.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y).sortByKey().collect()

cpu_values = [x[0] for x in cpu_distribution] 
cpu_counts = [x[1] for x in cpu_distribution]

# Create directory if it does not exist
os.makedirs("./src/question_1/plots", exist_ok=True)

# Plot results 
x_pos = range(len(cpu_values))

plt.figure(figsize=(8, 5))
plt.bar(x_pos, cpu_counts, width=0.6)

plt.xticks(x_pos, cpu_values)
plt.xlabel("CPU capacity (normalized)")
plt.ylabel("Number of machines")
plt.title("Distribution of Machine CPU Capacities")

plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig("./src/question_1/plots/cpu_capacity_distribution.png", dpi=300)