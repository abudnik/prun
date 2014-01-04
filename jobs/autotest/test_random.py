import os
import time

def RunManyJobs():
    # run random-success jobs
    jobPath = 'autotest/random_fail.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # run always failing jobs
    jobPath = 'autotest/fail.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def StopAll():
    cmd = 'python admin.py -c "stopall"'
    os.system( cmd )

for i in range(0, 5):
    RunManyJobs()
    time.sleep( i )
    StopAll()

RunManyJobs()

print( 'done' )
