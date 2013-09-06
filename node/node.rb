errCode = 0

begin
  shmemPath = ARGV[1]
  scriptLen = Integer(ARGV[2])
  shmemOffset = Integer(ARGV[3])
  taskId = ARGV[4]
  numTasks = ARGV[5]

  file = File.open(shmemPath, 'r')
  file.seek(shmemOffset, IO::SEEK_SET)
  buffer = file.read(scriptLen)

  inject = "taskId=" + taskId + "\n"
  inject += "numTasks=" + numTasks + "\n"

  eval(inject+buffer)
rescue Exception => e
  errCode = -1
  puts e.message
  puts e.backtrace.inspect
end

begin
  fifoName = ARGV[0]
  fifo = File.open(fifoName, 'w')
  fifo.write( [errCode].pack('V') )
  fifo.close()
rescue Exception => e
  puts e.message
  puts e.backtrace.inspect
end
