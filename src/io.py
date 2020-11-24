import logging

class IO(object):

    def __init__(self, filename):
        self.op_cnt = 0
        self.filename = filename
        self.operations = []
        self._read_in_ops()

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

    def print_var(self, name, value):
        """ print a variable """
        print("%s: %s" % (name, value))

    def report_transaction(self, name, can_commit):
        """ report status of a transaction """
        print("Transaction %s can commit: %s" % (name, can_commit))

    def dump(self, site_snapshot):
        """ print a snapshot of sites """
        for site_index in site_snapshot.keys():
            print("site %d - " % site_index, end='')
            for var_name in site_snapshot[site_index].keys():
                print("%s: %s, " % (var_name, site_snapshot[site_index][var_name]), end='')
            print('\n')
            
