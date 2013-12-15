echo "Chunk merging process started"
echo "taskId="$taskId", numTasks="$numTasks", jobId="$jobId

chunks=`ls -d data/*[0-9]`
outFile="data/output.txt"

sort --buffer-size=33% -T "data" -m $chunks > $outFile
errCode=$?

exit $errCode

