import os

path = '/home/budnik/prun/'

def RunTests():
    # run many simple jobs
    jobPath = path + 'test/autotest/simple_one_cpu.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    jobPath = path + 'test/autotest/simple_many_cpu.job'
    task = ''
    for i in range(0, 500):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # send many large source code files
    jobPath = path + 'test/autotest/many_code.job'
    task = ''
    for i in range(0, 1):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # run many medium jobs
    jobPath = path + 'test/autotest/medium.job'
    task = ''
    for i in range(0, 50):
        task += 'run ' + jobPath + ';'
    cmd = 'python admin.py -c "' + task + '"'
    os.system( cmd )

    # run many heavy jobs
    jobPath = path + 'test/autotest/heavy.job'
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

RunTests()
AddExistingUser()
StopAll()

print( 'done' )
