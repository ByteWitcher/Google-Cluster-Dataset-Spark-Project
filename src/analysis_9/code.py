import os
import matplotlib.pyplot as plt
from pyspark import SparkContext

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

# Task Usage
# 0,start time,INTEGER,YES
# 1,end time,INTEGER,YES
# 2,job ID,INTEGER,YES
# 3,task index,INTEGER,YES
# 4,machine ID,INTEGER,YES
# 5,CPU rate,FLOAT,NO
# 6,canonical memory usage,FLOAT,NO
# 7,assigned memory usage,FLOAT,NO
# 8,unmapped page cache,FLOAT,NO
# 9,total page cache,FLOAT,NO
# 10,maximum memory usage,FLOAT,NO
# 11,disk I/O time,FLOAT,NO
# 12,local disk space usage,FLOAT,NO
# 13,maximum CPU rate,FLOAT,NO
# 14,maximum disk IO time,FLOAT,NO
# 15,cycles per instruction,FLOAT,NO
# 16,memory accesses per instruction,FLOAT,NO
# 17,sample portion,FLOAT,NO
# 18,aggregation type,BOOLEAN,NO
# 19,sampled CPU usage,FLOAT,NO

# For each line of the input file, return a tuple (job ID, task index, machine ID, event type)
def parseTaskEventLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing machine ID value
    if splitLine[4] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[4]),
        int(splitLine[5]),
    )

# For each line of the input file, return a tuple (machine ID, CPU rate)
def parseTaskUsageLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing CPU rate value
    if splitLine[5] == "":
        return None

    return (
        int(splitLine[4]),
        float(splitLine[5]),
    )

#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")
task_usages = sc.textFile("./data/task_usage/part-0018*-of-00500.csv")

# Parse each line
parsed_task_events = (
    task_events.map(parseTaskEventLine).filter(lambda x: x is not None and x[3] == 2).map(lambda x: (x[2],1))
)
parsed_task_usages = (
    task_usages.map(parseTaskUsageLine).filter(lambda x: x is not None)
)

# Count evicted tasks per machine
evicted_task_count_per_machine = parsed_task_events.reduceByKey(lambda x, y: x + y)

# Average CPU usage per machine
mean_cpu_usage_per_machine = (
    parsed_task_usages
    .mapValues(lambda x: (x, 1))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda x: x[0] / x[1])  
)

# Join the two RDDs to get (machine_id, (mean_cpu_usage, evicted_task_count))
cpu_vs_evictions = (
    mean_cpu_usage_per_machine
    .join(evicted_task_count_per_machine)   # (machine_id, (cpu_usage, evictions))
    .map(lambda x: x[1]).collect()
)

cpu = [x[0] for x in cpu_vs_evictions]
evictions = [x[1] for x in cpu_vs_evictions]

# Create directory if it does not exist
os.makedirs("./src/analysis_9/plots", exist_ok=True)

# Plot results
plt.figure(figsize=(8, 6))
plt.scatter(cpu, evictions, alpha=0.5, s=10)

plt.xlabel("Mean CPU usage per machine")
plt.ylabel("Number of eviction events")
plt.title("CPU usage vs task evictions per machine")

plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("./src/analysis_9/plots/cpu_vs_evictions.png", dpi=300)