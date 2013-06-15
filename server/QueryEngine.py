import logging
import socket
from Transaction import Transaction

BUFFER = 1024

class QueryError(Exception):
    def __init__(self, value):
        self.value = value

class QueryEngine:
    """
    QueryEngine implements the client-server communication protocol.
    """

    def __init__(self, storage):
        """
        storage: Instance of StorageEngine
        """
        self.storage = storage
        self.logger = logging.getLogger(type(self).__name__)

    def start(self, socket, address):
        self.logger.debug('connected to: {}:{}'.format(address[0], address[1]))
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
                self.logger.error(msg)
                socket.send(msg)

            except BaseException as e:
                raise
                msg = str(e)
                self.logger.error(msg)
                socket.send(msg)

            except:
                socket.send("UNKNOWN ERROR")

        socket.close()
        self.logger.debug('disconnected from {}:{}'.format(address[0], address[1]))

    def parse(self, query):
        """
        Parses query, and returns (methodToCall, [args]).

        MethodToCall: reference to a StorageEngine method,
        Args:         list of args to pass to the StorageEngine method.
        """
        commands = {
            # command   : (methodToCall , argCount)
            'SET'       : ('set'        , 2),
            'GET'       : ('get'        , 1),
            'UNSET'     : ('unset'      , 1),
            'NUMEQUALTO': ('numequalto' , 1),
            'END'       : ('end'        , 0),
            'BEGIN'     : ('begin'      , 0),
            'ROLLBACK'  : ('rollback'   , 0),
            'COMMIT'    : ('commit'     , 0),
            'DUMP'      : ('dump'       , 0),
            'INDEX'     : ('show_index' , 0)
        }

        words = query.split()
        command = words[0].upper()
        args = words[1:]

        if command not in commands:
            raise QueryError("Unknown command '{}'".format(command))

        if len(args) != (commands[command][1]):
            error_msg = "Wrong number of arguments for {}; expected {} but got {}."
            raise QueryError(error_msg.format(command, commands[command][1], len(args)))

        return (commands[command][0], args)
