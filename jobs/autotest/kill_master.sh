#!/bin/bash

N=100
DELAY=3

for ((i=0; i<$N; i++)); do
    echo "starting master"
    ./pmaster --d
    sleep $DELAY
    echo "stopping master"
    ./pmaster --s
    #pkill pmaster
done
