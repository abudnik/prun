errCode = 0

begin
  shmemPath = ARGV[1]
  scriptLen = Integer(ARGV[2])
  shmemOffset = Integer(ARGV[3])
  taskId = Integer(ARGV[4])
  numTasks = Integer(ARGV[5])
rescue StandardError => e
  print e
end
