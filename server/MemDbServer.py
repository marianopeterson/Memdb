import logging
import socket
import thread


class MemDbServer(object):
    """
    Server: delegates client connections to QueryEngine threads.
    """

    def __init__(self, host, port, conns):
        self.host = host
        self.port = port
        self.conns = conns
        self.db = None
        self.protocol = None
        self.logger = logging.getLogger(type(self).__name__)
        
    def set_protocol(self, protocol):
        self.protocol = protocol
        
    def set_storage(self, storage):
        self.db = storage

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
                thread.start_new_thread(self.protocol.start, (client, address))

        except KeyboardInterrupt:
            print "\nShutting down."

        except socket.error, (value, message):
            self.logger.error('Could not open socket: {}'.format(message))

        finally:
            if server:
                server.close()
