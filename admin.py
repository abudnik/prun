import sys

def PrintHelp():
    print( "Commands:" )
    print( "  job /path/to/job/file -- run job, which described in .job file" )
    print( "  exit, e               -- quit program" )

def DoCommand( cmd ):
    print(cmd)
    if cmd == "help":
        PrintHelp()
        return

def UserPrompt():
    print( "master admin v0.1" )
    print( "print `help` for more information" )

def Main():
    UserPrompt();
    while True:
        sys.stdout.write( '> ' )
        sys.stdout.flush()
        line = sys.stdin.readline().strip()
        if line in ("exit", "e", "quit", "q"):
            break
        DoCommand( line )

Main()
