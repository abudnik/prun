from os import system
from time import sleep
from random import randrange

def RunManyJobs():
    # run many simple jobs
    jobPath = 'autotest/simple_one_cpu.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)

    jobPath = 'autotest/simple_many_cpu.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)
    
    # send many large source code files
    jobPath = 'autotest/many_code.job'
    task = ''
    for i in range(0, 1):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)

    # run many medium jobs
    jobPath = 'autotest/medium.job'
    task = ''
    for i in range(0, 50):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)

    # run many heavy jobs
    jobPath = 'autotest/heavy.job'
    task = ''
    for i in range(0, 10):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)

    # run many meta jobs
    jobPath = 'test.meta'
    task = ''
    for i in range(0, 50):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)

    # run cron jobs
    task = 'run cron.job; run cron.meta'
    cmd = './prun -c "' + task + '"'
    system(cmd)

def RunHeavyJobs():
    # run many heavy jobs
    jobPath = 'autotest/heavy.job'
    task = ''
    for i in range(0, 10):
        task += 'run ' + jobPath + ';'
    cmd = './prun -c "' + task + '"'
    system(cmd)

def AddExistingUser():
    task = ''
    for i in range(0, 100):
        task += 'add localhost groupx;'
    cmd = './prun -c "' + task + '"'
    system(cmd)

def StopAll():
    cmd = './prun -c "stopall"'
    system(cmd)

def DeleteGroup():
    task = 'deleteg groupx; deleteg hosts_group1'
    cmd = './prun -c "' + task + '"'
    system(cmd)

def AddGroup():
    task = 'addg hosts_group1'
    cmd = './prun -c "' + task + '"'
    system(cmd)

def JobInfo(jobId):
    task = 'info ' + str(jobId)
    cmd = './prun -c "' + task + '"'
    system(cmd)

def StopJob(jobId):
    task = 'stop ' + str(jobId)
    cmd = './prun -c "' + task + '"'
    system(cmd)

def Stat():
    task = 'stat; jobs; ls;'
    cmd = './prun -c "' + task + '"'
    system(cmd)


# check job stopping
RunManyJobs()
AddExistingUser()
StopAll()

for i in range(0, 10):
    RunManyJobs()
    StopAll()
sleep(2)
StopAll()

for i in range(0, 5):
    RunHeavyJobs()
    sleep(1)
    StopAll()

# check group removal
for i in range(0, 5):
    RunManyJobs()
    sleep(1)
    DeleteGroup()
    sleep(1)
    AddGroup()
    StopAll()

# check job info statistics and certain job stopping
RunManyJobs()

for i in range(0, 100):
    jobId = randrange(0, 1000)
    JobInfo(jobId)
    jobId = randrange(0, 1000)
    StopJob(jobId)

StopAll()

# check statistics
RunManyJobs()

for i in range(0, 1000):
    Stat()

print('done')
