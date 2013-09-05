#!/bin/bash

errCode=0

shmemPath=$2
scriptLen=$3
shmemOffset=$4
taskId=$5
numTasks=$6

s=`dd if=$shmemPath bs=1 skip=$shmemOffset count=$scriptLen 2>/dev/null`
if [ $? -eq 0 ]; then
eval "$s"
errCode=$?
else
errCode=-1
fi

fifoName=$1
printf "0: %.8x" $errCode | xxd -r -g0 > $fifoName
