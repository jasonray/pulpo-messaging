#!/bin/bash

# Default number of processes is 1 if not specified
num_processes=$1

# Truncate the log file
truncate -s 0 /tmp/pulpo/pulpo.log

# Loop to start the specified number of processes
for (( i=0; i<num_processes; i++ ))
do
    python3 pulpo-runner.py --config=pulpo-config.json >> /tmp/pulpo/pulpo.log 2>&1 &
done
