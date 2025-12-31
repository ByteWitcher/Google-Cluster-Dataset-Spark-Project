#!/bin/bash

# Create directories to store the downloaded data
mkdir -p data
mkdir -p data/machine_events
mkdir -p data/job_events
mkdir -p data/task_events
mkdir -p data/task_usage

# Download the schema file
gsutil cp gs://clusterdata-2011-2/schema.csv data/

# Download our parts of the dataset from Google Cloud Storage (from 180 to 184)
gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz data/machine_events

for i in {180..184}; do
  gsutil cp gs://clusterdata-2011-2/job_events/part-00${i}-of-00500.csv.gz data/job_events &
  gsutil cp gs://clusterdata-2011-2/task_events/part-00${i}-of-00500.csv.gz data/task_events &
  gsutil cp gs://clusterdata-2011-2/task_usage/part-00${i}-of-00500.csv.gz data/task_usage &
done

wait

# Unzip the downloaded files
gunzip data/machine_events/*.gz
gunzip data/job_events/*.gz
gunzip data/task_events/*.gz
gunzip data/task_usage/*.gz