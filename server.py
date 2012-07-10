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

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')
log = logging.getLogger(__name__)

''' Python synchronizes Dicts and Lists (i.e., they are thread-safe),
    UNLESS __eq__ or __hash__ have been overridden. For the purpose
    of this demo, we'll assume these methods have NOT been overridden.

    TODO: explicitly synchronize access to the database, by using threading.Lock.
'''

class MemDbServer:
    """ Server: Simply delegates client connections to QueryEngine threads.

                The client-server communication protocol is implemented by QueryEngine.
                The storage tasks are implemented by StorageEngine.
    """

    def __init__(self, host, port, conns):
        self.host = host
        self.port = port
        self.conns = conns
        self.db = StorageEngine()
        self.query_engine = QueryEngine(self.db)

    def start(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen(self.conns)
            while True:
                log.debug('waiting for connection...')
                client, address = server.accept()
                #TODO: limit the number of threads
                thread.start_new_thread(self.query_engine.start, (client, address))

        except KeyboardInterrupt:
            print "\nShutting down."

        except socket.error, (value, message):
            log.error('Could not open socket: {}'.format(message))

        finally:
            if server:
                server.close()


class QueryEngine:
    """ QueryEngine implements the client-server communication protocol.

        Connection handling is implemented by the MemDbServer.
        Storage is implemented by the StorageEngine.
    """

    def __init__(self, storage):
        """ storage: Instance of StorageEngine
        """
        self.storage = storage

    def start(self, socket, address):
        log.debug('connected to: {}:{}'.format(address[0], address[1]))
        while True:
            data = socket.recv(BUFFER)
            if not data:
                break
            try:
                log.debug("received: {}".format(data))
                command, args = self.parse(data)
                engine = getattr(self.storage, command)
                result = engine(*args)
                reply = 'OK' if result is None else str(result)
                socket.send(reply)
            except QueryError as e:
                msg = "ERROR: {}".format(e.value)
                log.error(e)
                socket.send(msg)

        socket.close()
        log.debug('disconnected from {}:{}'.format(address[0], address[1]))

    def parse(self, query):
        """ Parses query and returns (methodToCall, [args]).
            MethodToCall: reference to a StorageEngine method,
            Args:         list of args to pass to the StorageEngine method.
        """
        commands = {
            # command   : (methodToCall, argCount)
            'SET'       : ('set'        , 2),
            'GET'       : ('get'        , 1),
            'UNSET'     : ('unset'      , 1),
            'NUMEQUALTO': ('numequalto' , 1),
            'END'       : ('end'        , 0)
        }

        words = query.split()
        command = words[0].upper()
        args = words[1:]

        if command not in commands:
            raise QueryError("Unknown command '{}'".format(command))

        if len(args) != (commands[command][1]):
            raise QueryError("Wrong number of arguments for {}; expected {} but got {}.".format(command, commands[command][1], len(args)))

        return (commands[command][0], args)


class QueryError(Exception):
    def __init__(self, value):
        self.value = value


class StorageEngine:
    db = {}

    def __init__(self):
        #pass
        self.db = {} # key: value
        self.index = {} # value: [keys]

    def set(self, key, value):
        if key in self.db and self.index[self.db[key]]:
            # Remove key from index for this value
            del self.index[self.db[key]][key]
        self.db[key] = value
        if value in self.index:
            self.index[value][key] = True
        else:
            self.index[value] = {key: True}

    def get(self, key):
        if key in self.db:
            return self.db[key]
        return 'NULL'

    def unset(self, key):
        if key in self.db:
            if self.db[key] in self.index:
                # Remove key from index for this value
                del self.index[self.db[key]][key]
            del self.db[key]

    def numequalto(self, value):
        if value in self.index:
            # return ", ".join(map(str, self.index[value].keys()))
            return len(self.index[value])
        return 0

if __name__ == '__main__':
    server = MemDbServer(HOST, PORT, CONNS)
    server.start()