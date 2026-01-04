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

# For each line of the input file, return a tuple (job ID, task index, event type, priority)
def parseLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[5]),
        int(splitLine[8]),
    )

#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")

# Parse each line  to tuple (job ID, task index) as key and (priority, event type) as value
parsed_task_events = (
    task_events.map(parseLine).filter(lambda x: x is not None).map(lambda x: ((x[0], x[1]), (x[3], x[2])))
)

# For each priority, count the number of distinct tasks (job ID, task index)
tasks_per_priority = (
    parsed_task_events
    .map(lambda x: ((x[0], x[1][0]), 1))  # ((task_id, priority), 1)
    .distinct()
    .map(lambda x: (x[0][1], 1))          # (priority, 1)
    .reduceByKey(lambda x, y: x + y)
)

# For each priority, count the number of distinct evicted tasks (job ID, task index)
evicted_tasks_per_priority = (
    parsed_task_events
    .filter(lambda x: x[1][1] == 2)        # EVICT
    .map(lambda x: ((x[0], x[1][0]), 1))   # ((task_id, priority), 1)
    .distinct()
    .map(lambda x: (x[0][1], 1))           # (priority, 1)
    .reduceByKey(lambda x, y: x + y)
)

# Join the two RDDs and calculate eviction probability per priority
eviction_probability = (
    evicted_tasks_per_priority
    .join(tasks_per_priority)              # (priority, (evicted, total))
    .mapValues(lambda x: (x[0] / x[1]) * 100)
    .sortByKey()
    .collect()
)

# Print results
print("Eviction probability per priority level:")
for p, prob in eviction_probability:
    print(f"Priority {p}: {prob:.2f}%")

priorities = [x[0] for x in eviction_probability]
probabilities = [x[1] for x in eviction_probability]

# Create directory if it does not exist
os.makedirs("./src/analysis_13/plots", exist_ok=True)

# Plot results
plt.figure(figsize=(9, 5))
plt.bar(priorities, probabilities, width=0.6)
plt.xticks(priorities) 
plt.xlabel("Task priority")
plt.ylabel("Eviction probability (%)")
plt.title("Eviction probability by task priority")

plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("./src/analysis_13/plots/eviction_probability_by_priority.png", dpi=300)