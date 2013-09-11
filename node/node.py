import sys
import os
import mmap
import struct

NODE_SCRIPT_EXEC_FAILED = -5
errCode = 0

try:
    shmemPath = sys.argv[2]
    scriptLen = int(sys.argv[3])
    shmemOffset = int(sys.argv[4])
    taskId = int(sys.argv[5])
    numTasks = int(sys.argv[6])

    shmem = os.open( shmemPath, os.O_RDONLY )
    buf = mmap.mmap( shmem, scriptLen, mmap.MAP_SHARED, mmap.PROT_READ, offset=shmemOffset )
    s, = struct.unpack( str(scriptLen)+'s', buf[:scriptLen] )
    os.close( shmem )

    exec( s, {"taskId":taskId, "numTasks":numTasks} )
except Exception as e:
    errCode = NODE_SCRIPT_EXEC_FAILED
    print( e )

try:
    fifoName = sys.argv[1]
    fifo = os.open( fifoName, os.O_WRONLY | os.O_NONBLOCK )
    os.write( fifo, struct.pack('i', errCode) )
    os.close( fifo )
except Exception as e:
    print( e )
