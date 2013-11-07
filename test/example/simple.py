import math

def IsSimple( value ):
    if value % 2 == 0:
        return False

    root = math.sqrt( value )

    for i in range(3, int( root ), 2):
        if value % i == 0:
            return False

    return True

def main():
    #file = open( "out.txt", "w" )
    simples = []
    for i in range( 1, 500000 ):
        if IsSimple( i ):
            simples.append( i )
            #file.write( "%d\n" % i )

    #file.close()

    global taskId, numTasks
    print( "%i : %i" % (taskId, numTasks) )
    print( 'done' )

#import psyco
#psyco.full()

main()
