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

# For each line of the input file, return a tuple (job ID, task index, scheduling class, CPU request)
def parseTaskEventLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing scheduling class or CPU request  value
    if splitLine[7] == "" or splitLine[9] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[7]),
        float(splitLine[9]),
    )

# For each line of the input file, return a tuple (job ID, task index, CPU rate)
def parseTaskUsageLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing CPU rate value
    if splitLine[5] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        float(splitLine[5]),
    )

#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")
task_usages  = sc.textFile("./data/task_usage/part-0018*-of-00500.csv")

# Parse each line  to tuple (job ID, task index) as key and (CPU request, scheduling class) as value
parsed_task_events = (
    task_events.map(parseTaskEventLine).filter(lambda x: x is not None).map(lambda x: ((x[0], x[1]), (x[3], x[2])))
)

# Parse each line  to tuple (job ID, task index) as key and CPU rate as value
parsed_task_usages =  task_usages.map(parseTaskUsageLine).filter(lambda x: x is not None).map(lambda x: ((x[0], x[1]), x[2]) )

# Requested CPU per task (keep max request)
task_requests = (
    parsed_task_events
    .reduceByKey(lambda x, y: x if x[0] > y[0] else y)
)

# Mean CPU usage per task
task_mean_usage = (
    parsed_task_usages
    .mapValues(lambda x: (x, 1))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .mapValues(lambda x: x[0] / x[1])
    .filter(lambda x: x[1] > 0)
)

# CPU request over mean CPU usage per task, grouped by scheduling class
overestimation = (
    task_requests
    .join(task_mean_usage)
    .map(lambda x: (
        x[1][0][1],                  # scheduling class
        x[1][0][0] / x[1][1]         # request / usage
    ))
    .filter(lambda x: x[1] < 50)     # remove extreme outliers
)

# Average overestimation ratio per scheduling class
overestimation_by_class = (
    overestimation
    .mapValues(lambda x: (x, 1))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .mapValues(lambda x: x[0] / x[1])
    .sortByKey()    
    .collect()
)

# Print results
print("CPU Overestimation Ratio by Scheduling Class:")
for cls, ratio in overestimation_by_class:
    print(f"Scheduling class {cls}: {ratio:.2f}")


classes = [x[0] for x in overestimation_by_class]
ratios = [x[1] for x in overestimation_by_class]

# Create directory if it does not exist
os.makedirs("./src/analysis_11/plots", exist_ok=True)

# Plot results
plt.figure(figsize=(8, 5))
plt.bar(classes, ratios, width=0.6)

plt.xlabel("Scheduling class")
plt.ylabel("Average CPU overestimation ratio")
plt.title("CPU Request Overestimation by Scheduling Class")
plt.xticks(classes, [int(c) for c in classes]) 
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("./src/analysis_11/plots/cpu_overestimation_by_class.png", dpi=300)