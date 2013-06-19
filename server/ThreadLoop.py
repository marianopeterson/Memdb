import logging
import socket
from ProtocolEngine import ProtocolError
from Transaction import Transaction


BUFFER = 1024


class ThreadLoop(object):
    """
    Implements communication loop with a single connected client.
    Syntax and Storage implementations are handled externally and
    passed in via:
        start(socket, address, protocol, storage)
    """

    def __init__(self):
        self.logger   = logging.getLogger(type(self).__name__)

    def start(self, socket, address, protocol, storage):
        self.logger.debug('connected to: {}:{}'.format(address[0], address[1]))
        tx = None

        while True:
            data = socket.recv(BUFFER)
            if not data:
                break

            try:
                self.logger.debug("Received data: " + data)
                command_ref, args = protocol.parse(data, storage)
                result = command_ref(tx, *args)
                if isinstance(result, Transaction):
                    tx = result
                    result = None
                    if tx.state == Transaction.COMMITTED or tx.state == Transaction.ABORTED:
                        tx = None
                reply = 'OK' if result is None else str(result)
                socket.send(reply)

            except ProtocolError as e:
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