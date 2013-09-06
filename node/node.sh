#!/bin/bash

command_exists () {
    type "$1" &> /dev/null ;
}

NODE_SCRIPT_EXEC_FAILED=-5
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
