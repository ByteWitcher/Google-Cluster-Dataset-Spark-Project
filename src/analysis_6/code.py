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

# For each line of the input file, return a tuple (job ID, task index, event type, scheduling class)
def parseLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing scheduling class value
    if splitLine[7] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[5]),
        int(splitLine[7])
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

# Count the number of tasks for each scheduling class
task_count_per_scheduling_class = parsed_task_events.map(lambda x: (x[0],x[1],x[3])).distinct().map(lambda x: (x[2],1)).reduceByKey(lambda x,y: x+y)

# Count the number of evicted tasks for each scheduling class
evicted_task_count_per_scheduling_class = parsed_task_events.filter(lambda x: x[2] == 2).distinct().map(lambda x: (x[3],1)).reduceByKey(lambda x,y: x+y)

# Calculate eviction probability per scheduling class
eviction_probability_per_scheduling_class = evicted_task_count_per_scheduling_class.join(task_count_per_scheduling_class).mapValues(lambda x: (x[0]/x[1])*100)

# Print the results
for scheduling_class, eviction_probability in sorted(eviction_probability_per_scheduling_class.collect()):
    print(f"Scheduling Class {scheduling_class}: Eviction Probability = {eviction_probability:.2f}%")

results = sorted(eviction_probability_per_scheduling_class.collect())
classes = [x[0] for x in results]
probabilities = [x[1] for x in results]

# Create directory if it does not exist
os.makedirs("./src/analysis_6/plots", exist_ok=True)

# Plot results
plt.figure(figsize=(7, 5))
plt.bar(classes, probabilities, width=0.6)

plt.xlabel("Scheduling class")
plt.ylabel("Eviction probability (%)")
plt.title("Eviction Probability by Scheduling Class")

plt.xticks(classes)
plt.grid(axis="y", linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("./src/analysis_6/plots/eviction_probability_by_scheduling_class.png", dpi=300)