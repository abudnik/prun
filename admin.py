import sys
import socket

MASTER_PORT = 5557

def Exit(msg):
    print( msg )
    print( "exiting..." )
    sys.exit( 1 )

class Connection():
    def __init__(self):
        print( "connecting to master..." )
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect(('localhost', MASTER_PORT))
            print( "connected" )
        except Exception as e:
            Exit( "couldn't connect to master" )

    def Send(self, msg):
        try:        
            self.socket.send(msg)
        except Exception as e:
            Exit( "couldn't send command to master" )

class Master():
    def __init__(self, connection):
        self.connection = connection

    def DoCommand(self, cmd):
        #
        msg = '{"command":"job","file":"/home/budnik/dev/PythonServer/test/test.job"}'
        header = str(len(msg))+'\n'
        msg = header + msg
        self.connection.Send(msg)
        #
        print( "unknown command: " + cmd )

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
