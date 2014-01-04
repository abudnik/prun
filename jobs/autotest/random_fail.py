import random

RANGE = 10

if random.randrange(0,RANGE) == 0:
    print( "Throwing exception" )
    raise Exception( "random exception" )
