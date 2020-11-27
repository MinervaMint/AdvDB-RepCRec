import logging

class IO(object):

    def __init__(self, filename):
        self.op_cnt = 0
        self.filename = filename
        self.operations = []
        self._read_in_ops()

        logging.info("IO module initialized.")

    def _read_in_ops(self):
        """ read in operations from input source """
        with open(self.filename, 'r') as file:
            logging.info("Opened input file %s." % self.filename)
            ops = file.readlines()
            for op in ops:
                op.rstrip('\n')
            self.operations = ops
            logging.info("Read in %s operations." % len(self.operations))


    def get_op(self):
        """ return the next operation """
        if self.op_cnt > len(self.operations) - 1:
            return None 
        op = self.operations[self.op_cnt]
        self.op_cnt = self.op_cnt + 1
        return op

    @classmethod
    def print_var(cls, var_index, value):
        """ print a variable """
        print("x%s: %s" % (var_index, value))

    @classmethod
    def report_transaction(cls, transaction_index, can_commit):
        """ report status of a transaction """
        print("Transaction T%s can commit: %s" % (transaction_index, can_commit))

    @classmethod
    def dump(cls, site_snapshot):
        """ print a snapshot of sites """
        for site_index in site_snapshot.keys():
            print("site %d - " % site_index, end='')
            for var_index in site_snapshot[site_index].keys():
                print("x%s: %s, " % (var_index, site_snapshot[site_index][var_index]), end='')
            print('\n')
            
