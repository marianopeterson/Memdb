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

"""
Python synchronizes Dicts and Lists (i.e., they are thread-safe),
UNLESS __eq__ or __hash__ have been overridden. For the purpose
of this demo, we'll assume these methods have NOT been overridden.

TODO: explicitly synchronize access to the database, by using threading.Lock.
"""

class MemDbServer:
    """
    Server: Simply delegates client connections to QueryEngine threads.

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
    """
    QueryEngine implements the client-server communication protocol.

    Connection handling is implemented by the MemDbServer.
    Storage is implemented by the StorageEngine.
    """

    def __init__(self, storage):
        """ storage: Instance of StorageEngine
        """
        self.storage = storage

    def start(self, socket, address):
        log.debug('connected to: {}:{}'.format(address[0], address[1]))
        tx = None

        while True:
            data = socket.recv(BUFFER)
            if not data:
                break

            try:
                if not tx:
                    tx = self.storage.get_transaction()

                command, args = self.parse(data)

                # TODO: handle transaction commands: begin, commit, rollback
                if command == 'begin':
                    log.debug('BEGIN TRANSACTION')
                    socket.send('[TRANSACTION] OK')
                    continue
                elif command == 'rollback':
                    log.debug('ROLLBACK TRANSACTION')
                    socket.send('OK')
                    continue
                elif command == 'commit':
                    log.debug('COMMIT TRANSACTION')
                    socket.send('OK')
                    continue

                engine = getattr(self.storage, command)
                result = engine(tx, *args)
                reply = 'OK' if result is None else str(result)
                socket.send(reply)

            except QueryError as e:
                msg = "ERROR: {}".format(e.value)
                log.error(msg)
                socket.send(msg)

            except BaseException as e:
                raise
                msg = str(e)
                log.error(msg)
                socket.send(msg)

            except:
                socket.send("UNKNOWN ERROR")

        socket.close()
        log.debug('disconnected from {}:{}'.format(address[0], address[1]))

    def parse(self, query):
        """
        Parses query, and returns (methodToCall, [args]).

        MethodToCall: reference to a StorageEngine method,
        Args:         list of args to pass to the StorageEngine method.
        """
        commands = {
            # command   : (methodToCall, argCount)
            'SET'       : ('set'        , 2),
            'GET'       : ('get'        , 1),
            'UNSET'     : ('unset'      , 1),
            'NUMEQUALTO': ('numequalto' , 1),
            'END'       : ('end'        , 0),
            'BEGIN'     : ('begin'      , 0),
            'ROLLBACK'  : ('rollback'   , 0),
            'COMMIT'    : ('commit'     , 0),
            'DUMP'      : ('dump', 0)
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


class Transaction:
    def __init__(self, id):
        self.id = 0


class StorageEngine:

    def __init__(self):
        self.db = {}            # row_key: [tuple, ...]
        self.index = {}         # data_value: [key, ...]
        self.tx_next = 0        # next available tx id
        self.tx_last_commit = 0 # tx id of last commit
        self.tx_active = {}     # keys are ids of active (uncommitted) transactions
        self.tx_aborted = {}    # keys are ids of aborted transactions

    def get_transaction(self):
        """
        TODO: remove the race condition below (and make it thread-safe), by using threading.lock()
        """
        tx_id = self.tx_next
        self.tx_next += 1
        self.tx_active[tx_id] = True
        log.debug("BEGIN TRANSACTION {}".format(tx_id))
        return Transaction(tx_id)

    def dump(self, tx):
        return self.db

    def set(self, tx, key, value):
        # Don't remove keys from the index. Instead, rely on the read
        # methods to validate each key referenced by the index.
        # TODO: create a garbage collector to compact the index

        # Add tuple to this row
        log.debug("tx({}): set {}={}".format(tx.id, key, value))
        row_tuple = {'xmin': tx.id, 'xmax': None, 'value': value}
        if key in self.db:
            self.db[key].append(row_tuple)
        else:
            self.db[key] = [row_tuple]

        # Add this key to index for this value
        if value in self.index:
            self.index[value][key] = True
        else:
            self.index[value] = {key: True}

    def get(self, tx, key):
        """
        Rules to determine if a tuple is visible:

            The transaction that created the tuple (xmin) is:
                - not an aborted transacton
                - less than the current transaction's id (tx.id)
                - not in an active transaction when the current transaction began

            AND, the tuple's expiration transaction (xmax) is:
                - blank, or refs an aborted transaction
                - greater than the current transaction id
                - in an active transaction when the current transaction began
        """
        if key in self.db:
            for row in reversed(self.db[key]):
                xmin, xmax = row['xmin'], row['xmax']
                log.debug("StorageEngine.get() evaluating tuple: {}".format(row))
                if ((xmin == tx.id or \
                        ((xmin not in self.tx_aborted) and \
                        (xmin <= tx.id) and \
                        (xmin not in self.tx_active) and \
                        (xmax is None or \
                            (xmax in self.tx_aborted) or \
                            xmax > tx.id or \
                            xmax in self.tx_active)))):
                                log.debug("found visible row: {}".format(row))
                                return row['value']
        log.debug("returning NULL")
        return 'NULL'

    def unset(self, tx, key):
        if key in self.db:
            # Don't remove keys from the index. Instead, rely on the read
            # methods to validate each key referenced by the index.
            # TODO: create a garbage collector to compact the index

            # Add a tuple to this row, logging the expiration of the row.
            # TODO: rather than add another tuple, we should just find the current tuple and expire it.
            row_tuple = {'xmin': tx.id, 'xmax': tx.id, 'value': value}
            if key in self.db:
                self.db[key].append(row_tuple)

    def numequalto(self, tx, value):
        count = 0
        if value in self.index:
            for key in self.index:
                if self.get(tx, key) != 'NULL':
                    count += 1
        return count

if __name__ == '__main__':
    server = MemDbServer(HOST, PORT, CONNS)
    server.start()