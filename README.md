# Google-Cluster-Dataset-Spark-Project

- By: Soufiane ABAHAMID - Mohamed Quehlaoui
- Team: AK
- Data Subset: 180 - 184

# Anaylsis 1: Distribution of Machine CPU Capacities

To better understand the hardware heterogeneity of the cluster, we first analyzed the distribution of CPU capacities across all machines. The data was extracted from the **machine events** table, which provides information about each machine’s available resources over time. For each machine, we retained only the most recent event (the one with the latest timestamp) in order to represent its final known configuration during the trace period. Machine events corresponding to removal events were excluded from the analysis.

The CPU capacity values in the dataset are normalized, meaning that a value of 1.0 corresponds to the machine with the highest number of CPU cores in the cluster. Lower values represent machines with proportionally fewer cores (according to the google's documentation).

![Alt text describing the image](src/question_1/plots/cpu_capacity_distribution.png)

_Figure 1: Distribution of Machine CPU Capacities._

The results indicate that the majority of machines have a normalized CPU capacity of 0.5, suggesting a strong hardware homogeneity in the cluster. A smaller number of machines have higher capacities (around 1.0), which likely correspond to more powerful nodes intended for heavier workloads. Conversely, a very small fraction of machines have low CPU capacity (around 0.25), possibly representing older or specialized machines.

Overall, this distribution suggests that the cluster is largely composed of mid-range machines, with limited hardware heterogeneity. This design likely simplifies scheduling decisions and helps ensure predictable performance across tasks, while still allowing some flexibility through the presence of higher-capacity nodes.
