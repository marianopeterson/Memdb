import argparse
import logging
import sys

from server import MemDbServer
from server import StorageEngine
from server import QueryEngine

def main():
    parser = argparse.ArgumentParser(description='Starts a memory backed key-value database.')
    parser.add_argument('-H', '--host', default='localhost', type=str, help='Hostname on which to listen.')
    parser.add_argument('-p', '--port', default=5000, type=int, help='Port on which to listen.')
    parser.add_argument('-c', '--conns', default=5, type=int, help='Max number of connections.')
    opts = parser.parse_args()

    # TODO figure out cross-platform logging (i.e., pass log object into sub modules)
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) (%(name)s) %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    storage = StorageEngine()
    protocol = QueryEngine(storage)

    server = MemDbServer(opts.host, opts.port, opts.conns)
    server.set_protocol(protocol)
    server.set_storage(storage)
    server.start()

if __name__ == '__main__':
    status = main()
    sys.exit(status)