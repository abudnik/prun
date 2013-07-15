from socket import *               

def Send(msg, times):
	for i in range(times):
		try:
			s = socket( AF_INET, SOCK_STREAM )
			s.connect( ('127.0.0.1', 5555) )
			s.send( msg )
			print s.recv(1024)
			s.close()
		except Exception as e:
			print e

g = code = open('example.py', 'r').read()
for i in range(50000):
        print i
        g = "#" + g
        msg = str(len(g)) + "\n" + g
        Send( msg, 1 )

print "done"
