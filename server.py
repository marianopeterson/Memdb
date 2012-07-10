import argparse
import logging
import socket
import sys
import thread

parser = argparse.ArgumentParser()
parser.add_argument('-H', '--host', default='localhost', type=str, help='Hostname on which to listen.')
parser.add_argument('-p', '--port', default=5000, type=int, help='Port on which to listen.')
parser.add_argument('-c', '--conns', default=5, type=int, help='Max number of connections.')
args = parser.parse_args()

HOST   = args.host
PORT   = args.port
CONNS  = args.conns
BUFFER = 1024

logging.basicConfig(
    level=logging.DEBUG,
    #format="[%(asctime)s][%(levelname)8s][%(threadName)s][%(name)s] %(message)s")
    format='[%(levelname)s] (%(threadName)-10s) %(message)s')
log = logging.getLogger(__name__)


''' Python synchronizes Dicts and Lists (i.e., they are thread-safe),
    UNLESS __eq__ or __hash__ have been overridden. For the purpose
    of this demo, we'll assume these methods have NOT been overridden.

    TODO: explicitly synchronize access to the database, by using threading.Lock.
'''
db = []
server = None

class MemDbServer:
    """Server: delegate client connections to QueryEngine, in new threads.
    """

    def __init__(self, host, port, conns):
        self.host = host
        self.port = port
        self.conns = conns

    def start(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen(self.conns)
            while True:
                log.debug('waiting for connection...')
                client, address = server.accept()
                log.debug('connection from: {}'.format(address))
                thread.start_new_thread(QueryEngine().start, (client, address)) # TODO: pass the QueryParser in

        except KeyboardInterrupt:
            print "\nShutting down. DB contents are: ", db
        except socket.error, (value, message):
            log.error('Could not open socket: {}'.format(message))
        finally:
            if server:
                server.close()
            if client:
                client.close()

class QueryEngine:

    def __init__(self):
        pass

    def start(self, socket, address):
        se = StorageEngine()
        while True:
            data = socket.recv(BUFFER)
            if not data:
                break
            try:
                log.debug("Received: {}".format(data))
                command = self.parse(data)
                engine = getattr(se, command[0])
                result = engine(*command[1])
                socket.send('ack')
            except InvalidQueryException as e:
                log.error("Error: ", e)

        socket.close()
        log.debug('disconnected from {}'.format(address))

    def parse(self, query):
        """Parses query, and returns: (runnable, [args])
           Where RUNNABLE is a reference to a StorageEngine method,
           and ARGS is a list of args to pass to RUNNABLE.
        """
        commands = {
            # command   : (methodToCall, argCount)
            'SET'       : ('set', 3),
            'GET'       : ('get', 2),
            'UNSET'     : ('unset', 2),
            'NUMEQUALTO': ('numequalto', 2),
            'END'       : ('end', 1)
        }

        words = query.split()
        command = words[0].upper()

        if command not in commands:
            raise InvalidQueryException("Invalid query: '{}'. Unknown command '{}'".format(query, command))

        if len(words) != (commands[command][1]):
            raise InvalidQueryException(
                "Invalid query: '{}'. Command {} requires {} args, got {}."
                    .format(query, command, commands[command][1], len(words)-1))

        return (commands[command][0], words[1:])


class InvalidQueryException(Exception):
    pass

class StorageEngine:
    db = {}

    def __init__(self):
        pass

    def set(self, *args):
        log.debug("setting {}={}".format(args[0], args[1]))
        self.db[args[0]] = args[1]

    def get(self, name):
        if name in self.db:
            return self.db[name]
        return 'NULL'

if __name__ == '__main__':
    server = MemDbServer(HOST, PORT, CONNS)
    server.start()
