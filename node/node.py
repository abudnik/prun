import sys
import os
import struct


def Main():
    try:
        fifoName = sys.argv[1]
        fifo = open( fifoName, "wb" )
        print >> fifo, struct.pack('i', 0)
        fifo.close()
        exit(0)
    except Exception as e:
        print e

Main()
