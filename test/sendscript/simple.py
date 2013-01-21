import math

def IsSimple( value ):
    if value % 2 == 0:
        return False
    
    root = math.sqrt( value )
    
    for i in xrange(3, int( root ), 2):
        if value % i == 0:
            return False
        
    return True

def main():
    #file = open( "out.txt", "w" )
    
    for i in xrange( 1, 1000000 ):
        if IsSimple( i ):
			pass
            #file.write( "%d\n" % i )
            
    #file.close()
    
    print 'done'
    
#import psyco
#psyco.full()

main()
