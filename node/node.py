import sys
import os
import struct


def Main():
    try:
        fifoName = sys.argv[1]
        fifo = os.open(fifoName, os.O_WRONLY | os.O_NONBLOCK)
        os.write( fifo, struct.pack('i', 0) )
        os.close( fifo )
    except Exception as e:
        print e

Main()
