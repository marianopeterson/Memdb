import argparse
import socket
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-H', '--host', default='localhost', type=str, help='IP address of the server to connect to.')
parser.add_argument('-p', '--port', default=5000, type=int, help='Port to connect to on the server.')
args = parser.parse_args()

HOST   = args.host
PORT   = args.port
BUFFER = 1024

server = None
try:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((HOST, PORT))
    while True:
        data = raw_input('')
        if data.upper() == 'END':
            raise KeyboardInterrupt()
        server.send(data)
        response = server.recv(BUFFER)
        if response != 'OK':
            print ">> {}".format(response)

except socket.error, (value, message):
    print "Could not connect to socket: " + message

except KeyboardInterrupt:
    print "\nGood bye!"

finally:
    if server:
        server.close()