import logging
import socket
import thread


class ConnectionHandler(object):
    """
    Connection handler. Delegates each client connection to a socket
    server thread. The default thread implementation is ThreadLoop,
    but this can be overridden using ConnectionHandler.set_thread().
    """

    def __init__(self, host, port, conns):
        self.host = host
        self.port = port
        self.conns = conns

        self.thread = None
        self.storage = None
        self.protocol = None

        self.logger = logging.getLogger(type(self).__name__)

    def set_thread(self, thread):
        self.thread = thread

    def set_protocol(self, protocol):
        self.protocol = protocol

    def set_storage(self, storage):
        self.storage = storage

    def start(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen(self.conns)
            while True:
                self.logger.debug('waiting for connection...')
                client, address = server.accept()
                #TODO: limit the number of threads
                thread.start_new_thread(self.thread.start, (client, address, self.protocol, self.storage))

        except KeyboardInterrupt:
            print "\nShutting down."

        except socket.error, (value, message):
            self.logger.error('Could not open socket: {}'.format(message))

        finally:
            if server:
                server.close()