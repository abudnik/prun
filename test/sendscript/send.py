from socket import *               

code = open('example.py', 'r').read()
msg = str(len(code)) + "\n" + code

try:
    s = socket( AF_INET, SOCK_STREAM )
    s.connect( ('127.0.0.1', 5555) )
    s.send( msg )
    s.close()
except Exception as e:
    print e
