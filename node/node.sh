#!/bin/sh

command_exists () {
    command -v "$1" >/dev/null 2>&1
}

NODE_SCRIPT_EXEC_FAILED=-5
errCode=0

readFifo=$2
scriptLen=$3
taskId=$4
numTasks=$5
jobId=$6

s=`dd if=$readFifo bs=$scriptLen count=1 2>/dev/null`
if [ $? -eq 0 ]; then
    eval "$s"
    errCode=$?
else
    errCode=$NODE_SCRIPT_EXEC_FAILED
fi

fifoName=$1

#not sure if xxd exists on target system

if command_exists xxd ; then
    printf "0: %.8x" $errCode | xxd -r -g0 > $fifoName
else
    if [ $errCode -eq 0 ]; then
        printf "\x00\x00\x00\x00\x00\x00\x00\x00" > $fifoName
    else
        printf "\xff\xff\xff\xff\xff\xff\xff\xfb" > $fifoName
    fi
fi
