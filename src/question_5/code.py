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


# For each line of the input file, return a tuple (job ID, event type)
def parseJobLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    return (
        int(splitLine[2]),
        int(splitLine[3]),
    )


# For each line of the input file, return a tuple (job ID, task index, event type)
def parseTaskLine(line):
    # Split the line by commas and strip whitespace
    splitLine = [x.strip() for x in line.split(",")]

    return (
        int(splitLine[2]),
        int(splitLine[3]),
        int(splitLine[5]),
    )


#### Driver program

# Start spark
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

# Read the input file into an RDD[String]
job_events = sc.textFile("./data/job_events/part-0018*-of-00500.csv")
task_events = sc.textFile("./data/task_events/part-0018*-of-00500.csv")

# Parse each line
parsed_job_events = (
    job_events.map(parseJobLine).filter(lambda x: x is not None).distinct()
)
parsed_task_events = (
    task_events.map(parseTaskLine).filter(lambda x: x is not None).distinct()
)

# Count the total number of unique jobs and the number of unique jobs that were evicted or killed
total_jobs = parsed_job_events.map(lambda x: x[0]).distinct().count()
total_evicted_or_killed_jobs = (
    parsed_job_events.filter(lambda x: x[1] in [2, 5])
    .map(lambda x: x[0])
    .distinct()
    .count()
)

# Count the total number of unique tasks and the number of unique tasks that were evicted or killed
total_tasks = parsed_task_events.map(lambda x: (x[0], x[1])).distinct().count()
total_evicted_or_killed_tasks = (
    parsed_task_events.filter(lambda x: x[2] in [2, 5])
    .map(lambda x: (x[0], x[1]))
    .distinct()
    .count()
)

# Calculate the percentage of jobs and tasks that were evicted or killed
total_evicted_or_killed_jobs_percentage = (
    total_evicted_or_killed_jobs / total_jobs
) * 100
total_evicted_or_killed_tasks_percentage = (
    total_evicted_or_killed_tasks / total_tasks
) * 100

# Print the results
print(f"Total unique jobs: {total_jobs}")
print(
    f"Total unique jobs evicted or killed: {total_evicted_or_killed_jobs} ({total_evicted_or_killed_jobs_percentage:.2f}%)"
)
print(f"Total unique tasks: {total_tasks}")
print(
    f"Total unique tasks evicted or killed: {total_evicted_or_killed_tasks} ({total_evicted_or_killed_tasks_percentage:.2f}%)"
)
