import sys
import os
import mmap
import struct

NODE_SCRIPT_EXEC_FAILED = -5
errCode = 0

try:
    readFifo = sys.argv[2]
    scriptLen = int(sys.argv[3])
    taskId = int(sys.argv[4])
    numTasks = int(sys.argv[5])
    jobId = sys.argv[6]

    fifo = os.open( readFifo, os.O_RDONLY )
    bytes = bytearray()
    while len(bytes) < scriptLen:
        bytes += os.read( fifo, scriptLen )

    s = bytes.decode( "utf-8" )

    exec( s, {"taskId":taskId, "numTasks":numTasks, "jobId":jobId} )
except Exception as e:
    errCode = NODE_SCRIPT_EXEC_FAILED
    print( e )

try:
    writeFifo = sys.argv[1]
    fifo = os.open( writeFifo, os.O_WRONLY | os.O_NONBLOCK )
    os.write( fifo, str(errCode) )
    os.close( fifo )
except Exception as e:
    print( e )
