import os
import matplotlib.pyplot as plt
import numpy as np
from pyspark import SparkContext

# Job Events
# 0,time,INTEGER,YES
# 1,missing info,INTEGER,NO
# 2,job ID,INTEGER,YES
# 3,event type,INTEGER,YES : SUBMIT (0) | SCHEDULE (1) | EVICT(2) | FAIL(3) | FINISH(4) | KILL(5) | LOST(6) | UPDATE_PENDING(7) | UPDATE_RUNNING(8)
# 4,user,STRING_HASH,NO
# 5,scheduling class,INTEGER,NO : BETWEEN 0 AND 3
# 6,job name,STRING_HASH,NO
# 7,logical job name,STRING_HASH,NO

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


# For each line of the input file, return a tuple (job ID, scheduling class)
def parseJobLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing scheduling class value
    if splitLine[5] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[5]),
    )


# For each line of the input file, return a tuple (job ID, task index, scheduling class)
def parseTaskLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    # Skip rows with missing scheduling class value
    if splitLine[7] == "":
        return None

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[7]),
    )


#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
job_events = sc.textFile("./data/job_events/part-0018*-of-00500.csv")
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")

# Parse each line)
parsed_job_events = (
    job_events.map(parseJobLine).filter(lambda x: x is not None).distinct()
)
parsed_task_events = (
    task_events.map(parseTaskLine).filter(lambda x: x is not None).distinct()
)

# Count the number of jobs for each scheduling class
job_scheduling_class_counts = parsed_job_events.map(lambda x: (x[1], 1)).reduceByKey(
    lambda x, y: x + y
)
total_jobs = job_scheduling_class_counts.map(lambda x: x[1]).sum()
job_scheduling_class_counts = job_scheduling_class_counts.mapValues(
    lambda x: (x / total_jobs) * 100
)

job_results = job_scheduling_class_counts.sortByKey().collect()
scheduling_classes = [x[0] for x in job_results]
job_percentages = [x[1] for x in job_results]

# Count the number of tasks for each scheduling class
task_scheduling_class_counts = parsed_task_events.map(lambda x: (x[2], 1)).reduceByKey(
    lambda x, y: x + y
)
total_tasks = task_scheduling_class_counts.map(lambda x: x[1]).sum()
task_scheduling_class_counts = task_scheduling_class_counts.mapValues(
    lambda x: (x / total_tasks) * 100
)

task_results = task_scheduling_class_counts.sortByKey().collect()
task_percentages = [x[1] for x in task_results]

# Create directory if it does not exist
os.makedirs("./src/question_4/plots", exist_ok=True)

# Plot results
x = np.arange(len(scheduling_classes))
width = 0.35

plt.figure(figsize=(9, 5))

plt.bar(x - width / 2, job_percentages, width, label="Jobs")
plt.bar(x + width / 2, task_percentages, width, label="Tasks")

plt.xlabel("Scheduling class")
plt.ylabel("Percentage (%)")
plt.title("Jobs vs Tasks per scheduling class")

plt.xticks(x, scheduling_classes)
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.6)

plt.tight_layout()
plt.savefig("./src/question_4/plots/jobs_vs_tasks_per_class.png", dpi=300)
