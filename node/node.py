import sys
import os
import mmap
import struct


errCode = 0

try:
    shmemPath = sys.argv[2]
    scriptLen = int(sys.argv[3])
    shmemOffset = int(sys.argv[4])

    shmem = os.open( shmemPath, os.O_RDONLY )
    buf = mmap.mmap( shmem, scriptLen, mmap.MAP_SHARED, mmap.PROT_READ, offset=shmemOffset )
    s, = struct.unpack( str(scriptLen)+'s', buf[:scriptLen] )
    os.close( shmem )

    exec s in dict()
except Exception as e:
    errCode = -1
    print e

try:
    fifoName = sys.argv[1]
    fifo = os.open( fifoName, os.O_WRONLY )
    os.write( fifo, struct.pack('i', errCode) )
    os.close( fifo )
except Exception as e:
    print e
