import sys
import socket

MASTER_PORT = 5557

class Connection():
    def __init__(self):
        print( "connecting to master..." )
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect(('localhost', MASTER_PORT))
            print( "connected" )
        except Exception as e:
            print( "couldn't connect to master" )
            print( "exiting..." )
            sys.exit( 1 )

class Master():
    def __init__(self, connection):
        self.connection = connection

    def DoCommand(self, cmd):
        print( "unrecognized command: " + cmd )

def PrintHelp():
    print( "Commands:" )
    print( "  job /path/to/job/file -- run job, which described in .job file" )
    print( "  exit, e               -- quit program" )

def UserPrompt():
    print( "master admin v0.1" )
    print( "print `help` for more information" )

def Main():
    UserPrompt()
    con = Connection()
    master = Master( con )
    while True:
        sys.stdout.write( '> ' )
        sys.stdout.flush()
        line = sys.stdin.readline().strip()
        if len(line) == 0:
            continue
        if line in ("exit", "e", "quit", "q"):
            break
        if line == "help":
            PrintHelp()
            continue
        master.DoCommand( line )

Main()
