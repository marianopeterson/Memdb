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


class MemDbServer:
    """
    Server: Simply delegates client connections to QueryEngine threads.

    QueryEngine implements the client-server communication protocol.
    StorageEngine implements manipulation of data in the store.
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

    Connection handling is implemented by MemDbServer.
    Manipulation of data in the store is delegated to StorageEngine.
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
                command, args = self.parse(data)
                engine = getattr(self.storage, command)
                result = engine(tx, *args)
                if isinstance(result, Transaction):
                    tx = result
                    result = None
                    if tx.state == Transaction.COMMITTED or tx.state == Transaction.ABORTED:
                        tx = None
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
            'DUMP'      : ('dump'       , 0),
            'INDEX'     : ('show_index'      , 0)
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
    INPROGRESS = 1
    COMMITTED  = 2
    ABORTED    = 3

    def __init__(self, id, auto_commit=False, state=INPROGRESS):
        self.id = id
        self.auto_commit = auto_commit
        self.state = state


class StorageEngine:

    def __init__(self):
        self.db = {}            # row_key: [tuple, ...]
        self.index = {}         # data_value: [key, ...]
        self.tx_next = 0        # next available tx id
        self.tx_last_commit = 0 # tx id of last commit
        self.tx_active = {}     # keys are ids of active (uncommitted) transactions
        self.tx_aborted = {}    # keys are ids of aborted transactions

    def get_transaction(self, auto_commit=False):
        """
        TODO: remove the race condition below (and make it thread-safe), by using threading.lock()
        """
        tx_id = self.tx_next
        self.tx_next += 1
        self.tx_active[tx_id] = True
        tx = Transaction(tx_id, auto_commit=auto_commit)
        log.debug("tx({}): BEGIN".format(tx.id))
        return tx

    def commit(self, tx):
        if tx is None:
            log.debug("ERROR: no transaction to commit")
            return "ERROR: No transaction to commit."

        log.debug("tx({}): commit".format(tx.id))
        if tx.id in self.tx_active:
            del self.tx_active[tx.id]
        if tx.id > self.tx_last_commit:
            self.tx_last_commit = tx.id
        tx.state = Transaction.COMMITTED
        return tx

    def rollback(self, tx):
        if tx is None:
            log.debug("ERROR: no transaction to rollback.")
            return "INVALID ROLLBACK"

        self.tx_aborted[tx.id] = True
        if tx.id in self.tx_active:
            del self.tx_active[tx.id]
        tx.state = Transaction.ABORTED
        return tx

    def begin(self, tx):
        return self.get_transaction(auto_commit=False)

    def dump(self, tx):
        return self.db

    def show_index(self, tx):
        return self.index

    def set(self, tx, key, value):
        # We don't remove keys from the index. Instead, we rely on the
        # read methods to validate each key referenced by the index.
        # TODO: create a garbage collector to compact the index

        if tx is None:
            tx = self.get_transaction(auto_commit=True)

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

        if tx.auto_commit:
            self.commit(tx)

    def unset(self, tx, key):
        if tx is None:
            tx = self.get_transaction(auto_commit=True)

        if key in self.db:
            # We don't remove keys from the index. Instead, we rely on the
            # read methods to validate each key referenced by the index.
            # TODO: create a garbage collector to compact the index

            # Add a tuple to this row, logging the expiration of the row.
            # TODO: rather than add another tuple, we should just find the current tuple and expire it.
            row_tuple = {'xmin': tx.id, 'xmax': tx.id, 'value': None}
            if key in self.db:
                self.db[key].append(row_tuple)

        if tx.auto_commit:
            self.commit(tx)

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
        if tx is None:
            tx = Transaction(self.tx_last_commit)

        log.debug("tx({}): get {}".format(tx.id, key))

        if key in self.db:
            log.debug("tx({}): row {} has {} tuples".format(tx.id, key, len(self.db[key])))
            for row in reversed(self.db[key]):
                xmin, xmax = row['xmin'], row['xmax']
                log.debug("tx({}): evaluating tuple: {})".format(tx.id, row))
                if ((xmin == tx.id or \
                        ((xmin not in self.tx_aborted) and \
                        (xmin <= tx.id) and \
                        (xmin not in self.tx_active) and \
                        (xmax is None or \
                            (xmax in self.tx_aborted) or \
                            xmax > tx.id or \
                            xmax in self.tx_active)))):
                                log.debug("tx({}): found visible tuple: {})".format(tx.id, row))
                                if row['value'] is None:
                                    break
                                return row['value']
        log.debug("tx({}): found no visible tuples, returning NULL".format(tx.id))
        return 'NULL'

    def numequalto(self, tx, value):
        if tx is None:
            tx = Transaction(self.tx_last_commit)

        log.debug("tx({}): numequalto {}".format(tx.id, value))
        count = 0
        if value in self.index:
            """
            We treat the index like a bloom filter: row_keys that MIGHT have the value,
            are GUARANTEED to be in the index. However, the index may contain values
            that do not match the index, in the current transaction context.
            Consequently, we have to validate each row_key that the index maps to.
            """
            log.debug("tx({}): index points to {} possible matching rows".format(tx.id, len(self.index[value])))
            for row_key in self.index[value].iterkeys():
                if self.get(tx, row_key) == value:
                    count += 1
        log.debug("tx({}): found {} rows with value {}".format(tx.id, count, value))
        return count

if __name__ == '__main__':
    server = MemDbServer(HOST, PORT, CONNS)
    server.start()