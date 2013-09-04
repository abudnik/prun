import sys
import socket
import json
from threading import Thread

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

    def AddHeader(self, msg):
        header = str(len(msg)) + '\n'
        msg = header + msg
        return msg

    def Send(self, msg):
        msg = self.AddHeader( msg )
        try:
            self.socket.send( msg.encode( "utf-8" ) )
        except Exception as e:
            Exit( "couldn't send command to master" )

    def Receive(self):
        try:
            data = self.socket.recv( 32 * 1024 )
            return data
        except Exception as e:
            pass

    def Close(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except Exception as e:
            pass

class ResultGetter(Thread):
    def __init__( self, connection ):
        Thread.__init__(self)
        self.connection = connection
    
    def run(self):
        while True:
            msg = self.connection.Receive()
            if msg is None or len(msg) == 0:
                break
            print( msg.decode( "utf-8" ) )
            sys.stdout.write( '> ' )
            sys.stdout.flush()

class Command_Run():
    def Prepare(self, cmd):
        try:
            path = cmd.split()[1]
        except Exception as e:
            print( "no file path given" )
            raise e
        return json.JSONEncoder().encode( {"command" : "run", "file" : path} )

class Command_Info():
    def Prepare(self, cmd):
        try:
            jobId = int( cmd.split()[1] )
        except Exception as e:
            print( "invalid jobId argument" )
            raise e
        return json.JSONEncoder().encode( {"command" : "info", "job_id" : jobId} )

class Command_Stat():
    def Prepare(self, cmd):
        return json.JSONEncoder().encode( {"command" : "stat"} )

class Command_Test():
    def Prepare(self, cmd):
        msg = '{"command":"run","file":"/home/budnik/dev/prun/test/test.job"}'
        return msg


class CommandDispatcher():
    _instance = None
    def __init__(self):
        self.map_ = {'run'   : Command_Run(),
                     'info'  : Command_Info(),
                     'stat'  : Command_Stat(),
                     'test'  : Command_Test()}

    @classmethod
    def Instance(cls):
        if cls._instance is None:
            cls._instance = CommandDispatcher()
        return cls._instance

    def Get(self, command):
        cmd = command.split( None, 1 )[0]
        if cmd not in self.map_:
            return None
        return self.map_[ cmd ]

class Master():
    def __init__(self, connection):
        self.connection = connection

    def DoCommand(self, cmd):
        dispatcher = CommandDispatcher.Instance()
        handler = dispatcher.Get( cmd )
        if handler is not None:
            try:
                msg = handler.Prepare( cmd )
                self.connection.Send( msg )
            except Exception as e:
                print( "error: couldn't execute command" )
        else:
            print( "unknown command: " + cmd )

def PrintHelp():
    print( "Commands:" )
    print( "  run /path/to/job/file -- run job, which described in .job file" )
    print( "  info <job_id>         -- show job execution statistics" )
    print( "  stat                  -- show master statistics" )
    print( "  repeat, r             -- repeat last command" )
    print( "  exit, e               -- quit program" )

def UserPrompt():
    print( "master admin v0.1" )
    print( "print `help` for more information" )

def Main():
    UserPrompt()
    con = Connection()
    master = Master( con )
    resultGetter = ResultGetter( con )
    resultGetter.start()

    try:
        lastCmd = None
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
            if line in ("repeat", "r") and lastCmd is not None:
                line = lastCmd

            master.DoCommand( line )
            lastCmd = line
    except Exception as e:
        print( e )

    con.Close()
    resultGetter.join()

Main()
