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
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing CPU value
    if splitLine[4] == "":
        return None

    return (
        int(splitLine[0]),
        int(splitLine[1]),
        int(splitLine[2]),
        float(splitLine[4]),
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

    return lost


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
sorted_events_by_machine = events_by_machine.mapValues(
    lambda evts: sorted(evts, key=lambda x: x[0])
)

# Total lost CPU time across all machines
total_lost_cpu_time = (
    sorted_events_by_machine.mapValues(compute_lost_cpu_time).map(lambda x: x[1]).sum()
)

# Find the end time of the trace
trace_end_time = parsed_events.map(lambda x: x[0]).max()


# Compute total CPU time available in the cluster
def compute_total_cpu_time(events):
    for time, event, cpu in events:
        if event == 0:  # first ADD
            return cpu * (trace_end_time - time)
    return 0.0


# Total CPU time across all machines
total_cpu_time = (
    sorted_events_by_machine.mapValues(compute_total_cpu_time).map(lambda x: x[1]).sum()
)

# Compute percentage of computational power lost due to maintenance
percentage_lost = (total_lost_cpu_time / total_cpu_time) * 100
print(
    f"Percentage of computational power lost due to maintenance: {percentage_lost:.2f}%"
)
