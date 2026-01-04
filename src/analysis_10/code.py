from pyspark import SparkContext

# Machine Events
# 0,time,INTEGER,YES
# 1,machine ID,INTEGER,YES
# 2,event type,INTEGER,YES : ADD (0) | REMOVE (1) | UPDATE (2)
# 3,platform ID,STRING_HASH,NO
# 4,CPUs,FLOAT,NO
# 5,Memory,FLOAT,NO

# Task Events
# 0,time,INTEGER,YES
# 1,missing info,INTEGER,NO
# 2,job ID,INTEGER,YES
# 3,task index,INTEGER,YES
# 4,machine ID,INTEGER,NO
# 5,event type,INTEGER,YES : SUBMIT (0) | SCHEDULE (1) | EVICT(2) | FAIL(3) | FINISH(4) | KILL(5) | LOST(6) | UPDATE_PENDING(7) | UPDATE_RUNNING(8)
# 6,user,STRING_HASH,NO
# 7,scheduling class,INTEGER,NO : BETWEEN 0 AND 3
# 8,priority,INTEGER,YES : BETWEEN 0 AND 11
# 9,CPU request,FLOAT,NO
# 10,memory request,FLOAT,NO
# 11,disk space request,FLOAT,NO
# 12,different machines restriction,BOOLEAN,NO

# For each line of the input file, return a tuple (time, machine ID, CPU)
def parseMachineEventLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing CPU value
    if splitLine[4] == "":
        return None

    return (
        int(splitLine[0]),
        int(splitLine[1]),
        float(splitLine[4]),
    )

# For each line of the input file, return a tuple (time, machine ID, CPU delta)
def parseTaskEventLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing machine ID or CPU request value
    if splitLine[4] == "" or splitLine[9] == "":
        return None
    
    event_type = int(splitLine[5])
    cpu = float(splitLine[9])

    if event_type == 1: # SCHEDULE
        delta = cpu
    elif event_type in {2,3,4,5,6}: # end of execution
        delta = -cpu
    else:
        return None
    
    return (int(splitLine[0]),int(splitLine[4]),delta)

#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
machine_events = sc.textFile("./data/machine_events/part-00000-of-00001.csv")
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")

# Parse each line
parsed_machine_events = (
    machine_events.map(parseMachineEventLine).filter(lambda x: x is not None)
)
parsed_task_events = (
    task_events.map(parseTaskEventLine).filter(lambda x: x is not None)
)

# Create RDDs of the form (machine ID, (time, CPU)) and sort by time
machine_capacity_timeline = parsed_machine_events.map(lambda x: (x[1], (x[0], x[2]))).groupByKey().mapValues(lambda evs: sorted(evs, key=lambda x: x[0]))

# Create RDDs of the form (machine ID, (time, CPU delta)) and sort by time
machine_delta_timeline = parsed_task_events.map(lambda x: (x[1], (x[0], x[2]))).groupByKey().mapValues(lambda evs: sorted(evs, key=lambda x: x[0]))

# Join the two timelines on machine ID to get (machine ID, (capacity timeline, delta timeline))
joined = machine_capacity_timeline.join(machine_delta_timeline)

def overcommit_fraction(capacity_events, cpu_events):
    """
    capacity_events: [(t, cap)]
    cpu_events: [(t, delta)]
    """

    # Merge all timestamps
    times = sorted(
        set([t for t,_ in capacity_events] + [t for t,_ in cpu_events])
    )

    cap_idx = 0
    cpu_idx = 0
    capacity = capacity_events[0][1]
    cpu = 0.0

    over_time = 0
    total_time = 0

    for i in range(len(times) - 1):
        t = times[i]
        dt = times[i+1] - times[i]

        # update capacity
        while cap_idx < len(capacity_events) and capacity_events[cap_idx][0] <= t:
            capacity = capacity_events[cap_idx][1]
            cap_idx += 1

        # update cpu
        while cpu_idx < len(cpu_events) and cpu_events[cpu_idx][0] <= t:
            cpu += cpu_events[cpu_idx][1]
            cpu_idx += 1

        total_time += dt
        if cpu > capacity:
            over_time += dt

    if total_time == 0:
        return 0.0

    return over_time / total_time

# Compute per-machine over-commitment
per_machine_overcommit = (
    joined
    .mapValues(lambda x: overcommit_fraction(x[0], x[1]))
)

# Average fraction of time machines are over-committed
average_overcommit_fraction = per_machine_overcommit.values().mean()

print("Average fraction of time machines are over-committed:", average_overcommit_fraction)
