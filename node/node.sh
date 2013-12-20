NODE_SCRIPT_EXEC_FAILED=-5
errCode=0

readFifo=$2
scriptLen=$3
taskId=$4
numTasks=$5
jobId=$6

s=`dd if=$readFifo bs=$scriptLen count=1 2>/dev/null`
if [ $? -eq 0 ]; then
    (eval "$s")
    errCode=$?
else
    errCode=$NODE_SCRIPT_EXEC_FAILED
fi

fifoName=$1
echo -n $errCode > $fifoName
