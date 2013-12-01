import os

path = '/home/andrew/prun/'

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

print( 'done' )
