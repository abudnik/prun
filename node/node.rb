errCode = 0

begin
  shmemPath = ARGV[1]
  scriptLen = Integer(ARGV[2])
  shmemOffset = Integer(ARGV[3])
  taskId = Integer(ARGV[4])
  numTasks = Integer(ARGV[5])

  file = File.open(shmemPath, "r")
  file.seek(shmemOffset, IO::SEEK_SET)
  buffer = file.read(scriptLen)

  errCode = eval(buffer)
rescue StandardError => e
  print e
  errCode = -1
end

begin
  fifoName = ARGV[0]
  fifo = File.open(fifoName, "w")
  fifo.write( errCode )
  fifo.close()
rescue StandardError => e
  print e
end
