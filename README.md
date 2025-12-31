# Google-Cluster-Dataset-Spark-Project

Soufiane ABAHAMID
Mohamed Quehlaoui
Team: AK
Data Subset: 180 - 184

# Notes

- The very first thing we did was install pyenv, since gsutil requires version 3.13 , and all we had was 3.12.
- After that we installed the python 3.13 version and gsutil
- Then we downloaded our parts from `e` - ``:
  - part-00180-of-00500.csv.gz
  - part-00181-of-00500.csv.gz
  - part-00182-of-00500.csv.gz
  - part-00183-of-00500.csv.gz
  - part-00184-of-00500.csv.gz

# Anaylsis 1

Distribution of the machines according to their CPU capacity :

For this analysis there is one single file, about the whole Google Cell so we executed the code on the full Dataset.

Based on the google documentation there are 3 types of machine events; ADD, REMOVE and UPDATE, naturally we should ignore the REMOVE events since a prior record concerning the machine would indicate the same resources,and since a machine can start with a certain capacity and get updated later, then for each machine we should only keep the latest record either an ADD (because it could get removed earlier and added again, a restart) or UPDATE, by comparing their timestamps and finally when we have all the needed records, we would group them by their CPUs field, and plot the result.
