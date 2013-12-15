echo "Sorting chunk process started"
echo "taskId="$taskId", numTasks="$numTasks", jobId="$jobId

filename="data/input.txt"
outFile="data/$taskId"

fileSize=`stat --printf="%s" $filename`
partSize=`expr $fileSize / $numTasks`

dd if=$filename bs=$partSize skip=$taskId count=1 | sort --buffer-size=$partSize"b" > $outFile
errCode=${PIPESTATUS[0]}

exit $errCode
