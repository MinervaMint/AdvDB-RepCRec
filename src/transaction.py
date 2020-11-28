import os
import logging
from enum import Enum

class Transaction(object):

    TStatus = Enum("TStatus", ('Running', 'Blocked', 'Committed', 'Aborted'))

    def __init__(self, index, read_only, start_time):
        self.index = index
        self.uncommitted_vars = {}
        self.status = self.TStatus.Running
        self.read_only = read_only
        self.start_time = start_time

    def write_uncommitted(self, var_index, value):
        """ store write value in transaction """
        self.uncommitted_vars[var_index] = value
        logging.info("Write x%s = %s to uncommitted variables in T%s." % (var_index, value, self.index))
