import logging
import socket

class ProtocolError(Exception):
    def __init__(self, value):
        self.value = value

class ProtocolEngine:
    """
    Implements the client-server communication protocol, translating
    client syntax into method calls compatible with the storage engine.
    """
    _commands = {
        # command   : (methodToCall , argCount, help)
        'SET'       : ('set'        , 2       , '  SET [key] [value]'),
        'GET'       : ('get'        , 1       , '  GET [key]'),
        'UNSET'     : ('unset'      , 1       , '  UNSET [key]'),
        'NUMEQUALTO': ('numequalto' , 1       , '  NUMEQUALTO [value]  Returns the number of keys with corresponding [value]'),
        'END'       : ('end'        , 0       , '  END                 Disconnects client.'),
        'BEGIN'     : ('begin'      , 0       , '  BEGIN               Begins a new transaction. Commands will not be persisted until COMMIT is called.'),
        'ROLLBACK'  : ('rollback'   , 0       , '  ROLLBACK            Cancels an open transaction.'),
        'COMMIT'    : ('commit'     , 0       , '  COMMIT              Commits an open transaction.'),
        'DUMP'      : ('dump'       , 0       , '  DUMP                Prints all key/values in the store'),
        'INDEX'     : ('show_index' , 0       , '  INDEX               Prints all keys in the index')
    }

    def __init__(self):
        self.logger = logging.getLogger(type(self).__name__)

    def help(self, *args):
        """
        Returns message explaining the client syntax.
        TODO: sort commands in an intuitive order
        """
        msg = "\n".join([self._commands[key][2] for key in self._commands.keys()])
        return "\nCOMMANDS\n" + msg

    def parse(self, query, storage):
        """
        Maps client syntax to the storage engine's API.

        query:   String containing client syntax (e.g., "set key value").

        storage: Reference to a Storage engine object, which handles data
                 persistence and retrieval.

        Return: Tuple containing:

            method_ref: reference to an executable method, typically in the
                        storage engine. The method's signature must accept a
                        list of args: method(self, *args)

            args:       list of args that will be passed to method_ref.
        """
        words   = query.split()
        command = words[0].upper()
        args    = words[1:]
        
        if command == "HELP":
            return (self.help, [])

        if command not in self._commands:
            raise ProtocolError("Unknown command '{}'".format(command))

        """TODO: recognize quoted strings as a single argument:
                set key "hello world"  <--should count as 2 args, and be valid.
        """
        if len(args) != (self._commands[command][1]):
            error_msg = "Wrong number of arguments for {}; expected {} but got {}."
            raise ProtocolError(error_msg.format(command, self._commands[command][1], len(args)))
            
        cmd_ref = getattr(storage, self._commands[command][0])
        return (cmd_ref, args)