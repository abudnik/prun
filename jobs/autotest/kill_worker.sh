#!/bin/bash

N=100
DELAY=3

for ((i=0; i<$N; i++)); do
    echo "starting worker"
    ./pworker --d
    sleep $DELAY
    echo "stopping worker"
    ./pworker --s
    #pkill pworker
done
