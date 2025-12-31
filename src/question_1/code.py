from pyspark import SparkContext
import matplotlib.pyplot as plt

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

#### Driver program

# Start spark with 1 worker thread
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
events = sc.textFile("./data/machine_events/part-00000-of-00001.csv")

# Parse each line into a tuple (time, machine ID, event type, CPUs)
parsed_events = events.map(parseLine).filter(lambda x: x is not None)

# Filter out REMOVE events
filtered_events = parsed_events.filter(lambda x: x[2] != 1)

# Transfrom to key-value pairs of the form (machine ID, (time, CPUs))
machine_event_key_value_pairs = filtered_events.map(lambda x: (x[1], (x[0], x[3])) )

# For each machine ID, get the event with the latest time
most_recent_machine_events = machine_event_key_value_pairs.reduceByKey(lambda x,y: x if x[0] > y[0] else y)

# Extract the CPU values from the results
cpu_values = most_recent_machine_events.map(lambda x: x[1][1])

# Compute the distribution of CPU values
cpu_distribution = cpu_values.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y).sortByKey().collect()

# Plot results 
cpu_values = [x[0] for x in cpu_distribution]
counts = [x[1] for x in cpu_distribution]

plt.figure(figsize=(8, 5))
plt.bar(cpu_values, counts, width=0.1)

plt.xlabel("CPU capacity (normalized)")
plt.ylabel("Number of machines")
plt.title("Distribution of Machine CPU Capacities")

plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.savefig("./src/question_1/plots/cpu_capacity_distribution.png", dpi=300, bbox_inches="tight")