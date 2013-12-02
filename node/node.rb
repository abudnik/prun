NODE_SCRIPT_EXEC_FAILED = -5
errCode = 0

begin
  readFifo = ARGV[1]
  scriptLen = Integer(ARGV[2])
  taskId = ARGV[3]
  numTasks = ARGV[4]

  fifo = File.open(readFifo, 'r')
  buffer = ''
  while buffer.length < scriptLen do
      buffer += fifo.read( scriptLen )
  end

  inject = "taskId=" + taskId + "\n"
  inject += "numTasks=" + numTasks + "\n"

  eval(inject+buffer)
rescue Exception => e
  errCode = NODE_SCRIPT_EXEC_FAILED
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
