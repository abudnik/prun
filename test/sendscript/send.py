from socket import *               

def Send(msg, times):
	for i in range(1, times):
		try:
			s = socket( AF_INET, SOCK_STREAM )
			s.connect( ('127.0.0.1', 5555) )
			s.send( msg )
			print s.recv(1024)
			s.close()
		except Exception as e:
			print e

code = open('example.py', 'r').read()
msg = str(len(code)) + "\n" + code
Send( msg, 2 )

code = open('simple.py', 'r').read()
msg = str(len(code)) + "\n" + code
Send( msg, 2 )

#code = open('endless.py', 'r').read()
#msg = str(len(code)) + "\n" + code
#Send( msg )

print "done"
