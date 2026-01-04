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

# For each line of the input file, return a tuple (job ID, task index, machine ID, event type)
def parseLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing machine ID value
    if splitLine[4] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[4]),
        int(splitLine[5])
    )


#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")

# Parse each line
parsed_task_events = (
    task_events.map(parseLine).filter(lambda x: x is not None).distinct()
)

# Filter the RDD to only include SCHEDULE events (event type = 1)
schedule_events = parsed_task_events.filter(lambda x: x[3] == 1)

# For each job ID, count the number of distinct machines it has been scheduled on
machines_per_job = schedule_events.map(lambda x: (x[0], x[2])).distinct().map(lambda x: (x[0], 1)) .reduceByKey(lambda x, y: x + y) 

# For each distinct number of machines, count how many jobs have been scheduled on that many machines
machines_distribution = machines_per_job.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).sortByKey().collect()

x = [x[0] for x in machines_distribution]
y = [x[1] for x in machines_distribution]

# Create directory if it does not exist
os.makedirs("./src/analysis_7/plots", exist_ok=True)

# Plot results
plt.figure(figsize=(9, 5))
plt.bar(x, y, width=0.6)

plt.xlabel("Number of distinct machines per job")
plt.ylabel("Number of jobs")
plt.title("Task locality: machines used per job")

plt.yscale("log")  # VERY IMPORTANT (distribution is heavy-tailed)
plt.grid(axis='y', linestyle='--', alpha=0.6)

plt.tight_layout()
plt.savefig("./src/analysis_7/plots/machines_per_job.png", dpi=300)