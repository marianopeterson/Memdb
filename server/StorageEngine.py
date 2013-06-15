import logging
from Transaction import Transaction

class StorageEngine:

    def __init__(self):
        self.db = {}            # row_key: [tuple, ...]
        self.index = {}         # data_value: [key, ...]
        self.tx_next = 0        # next available tx id
        self.tx_last_commit = 0 # tx id of last commit
        self.tx_active = {}     # keys are ids of active (uncommitted) transactions
        self.tx_aborted = {}    # keys are ids of aborted transactions
        self.logger = logging.getLogger(type(self).__name__)

    def get_transaction(self, auto_commit=False):
        """
        TODO: remove the race condition below (and make it thread-safe), by using threading.lock()
        """
        tx_id = self.tx_next
        self.tx_next += 1
        self.tx_active[tx_id] = True
        tx = Transaction(tx_id, auto_commit=auto_commit)
        self.logger.debug("tx({}): BEGIN".format(tx.id))
        return tx

    def commit(self, tx):
        if tx is None:
            self.logger.debug("ERROR: no transaction to commit")
            return "ERROR: No transaction to commit."

        self.logger.debug("tx({}): commit".format(tx.id))
        if tx.id in self.tx_active:
            del self.tx_active[tx.id]
        if tx.id > self.tx_last_commit:
            self.tx_last_commit = tx.id
        tx.state = Transaction.COMMITTED
        return tx

    def rollback(self, tx):
        if tx is None:
            self.logger.debug("ERROR: no transaction to rollback.")
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
        self.logger.debug("tx({}): set {}={}".format(tx.id, key, value))
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

        self.logger.debug("tx({}): get {}".format(tx.id, key))

        if key in self.db:
            self.logger.debug("tx({}): row {} has {} tuples".format(tx.id, key, len(self.db[key])))
            for row in reversed(self.db[key]):
                xmin, xmax = row['xmin'], row['xmax']
                self.logger.debug("tx({}): evaluating tuple: {})".format(tx.id, row))
                if ((xmin == tx.id or \
                        ((xmin not in self.tx_aborted) and \
                        (xmin <= tx.id) and \
                        (xmin not in self.tx_active) and \
                        (xmax is None or \
                            (xmax in self.tx_aborted) or \
                            xmax > tx.id or \
                            xmax in self.tx_active)))):
                                self.logger.debug("tx({}): found visible tuple: {})".format(tx.id, row))
                                if row['value'] is None:
                                    break
                                return row['value']
        self.logger.debug("tx({}): found no visible tuples, returning NULL".format(tx.id))
        return 'NULL'

    def numequalto(self, tx, value):
        if tx is None:
            tx = Transaction(self.tx_last_commit)

        self.logger.debug("tx({}): numequalto {}".format(tx.id, value))
        count = 0
        if value in self.index:
            """
            We treat the index like a bloom filter: row_keys that MIGHT have the value,
            are GUARANTEED to be in the index. However, the index may contain values
            that do not match the index, in the current transaction context.
            Consequently, we have to validate each row_key that the index maps to.
            """
            self.logger.debug("tx({}): index points to {} possible matching rows".format(tx.id, len(self.index[value])))
            for row_key in self.index[value].iterkeys():
                if self.get(tx, row_key) == value:
                    count += 1
        self.logger.debug("tx({}): found {} rows with value {}".format(tx.id, count, value))
        return count
