import os
import time
import random

path = '/home/budnik/prun/'

def RunManyJobs():
    # run many simple jobs
    jobPath = path + 'jobs/autotest/simple_one_cpu.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    jobPath = path + 'jobs/autotest/simple_many_cpu.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # send many large source code files
    jobPath = path + 'jobs/autotest/many_code.job'
    task = ''
    for i in range(0, 1):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # run many medium jobs
    jobPath = path + 'jobs/autotest/medium.job'
    task = ''
    for i in range(0, 50):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # run many heavy jobs
    jobPath = path + 'jobs/autotest/heavy.job'
    task = ''
    for i in range(0, 10):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def RunHeavyJobs():
    # run many heavy jobs
    jobPath = path + 'jobs/autotest/heavy.job'
    task = ''
    for i in range(0, 10):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def AddExistingUser():
    task = ''
    for i in range(0, 100):
        task += 'add localhost groupx;'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def StopAll():
    cmd = 'python admin.py -c "stopall"'
    os.system( cmd )

def DeleteGroup():
    task = 'deleteg groupx; deleteg hosts_group1'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def AddGroup():
    task = 'addg ' + path + 'hosts_group1'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def JobInfo(jobId):
    task = 'info ' + str(jobId)
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def JobInfo(jobId):
    task = 'info ' + str(jobId)
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def StopJob(jobId):
    task = 'stop ' + str(jobId)
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

def Stat():
    task = 'stat; jobs; ls;'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )


# check job stopping
RunManyJobs()
AddExistingUser()
StopAll()

for i in range(0, 10):
    RunManyJobs()
    StopAll()
time.sleep(2)
StopAll()

for i in range(0, 5):
    RunHeavyJobs()
    time.sleep(1)
    StopAll()

# check group removal
for i in range(0, 5):
    RunManyJobs()
    time.sleep(1)
    DeleteGroup()
    time.sleep(1)
    AddGroup()
    StopAll()

# check job info statistics and certain job stopping
RunManyJobs()

for i in range(0, 100):
    jobId = random.randrange(0, 1000)
    JobInfo( jobId )
    jobId = random.randrange(0, 1000)
    StopJob( jobId )

StopAll()

# check statistics
RunManyJobs()

for i in range(0, 1000):
    Stat()

print( 'done' )
