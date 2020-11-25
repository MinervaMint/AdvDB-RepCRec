import os
import logging
from enum import Enum
from .lock import Lock

NUM_VARS = 20

class DataManager(object):

    VStatus = Enum("VStatus", ("Ready", "Unavailable", "Recovering"))

    def __init__(self, associated_site):
        self.associated_site = associated_site
        self.variables = {}
        self.variable_status = {}
        self.locktable = {}

        # init variables and variable status
        for i in range(1, NUM_VARS+1):
            if i % 2 == 0 or i % 10 + 1 == associated_site:
                self.variables[i] = i * 10
                self.variable_status[i] = VStatus.Ready



    def fail(self):
        """ when the corresponding site fails """
        # lock information is lost

        # change variable status

        pass

    def recover(self):
        """ when the corresponding site recovers """
        # change variable status

        pass

    def get_committed_var(self, var_index):
        """ get committed value of a variable """
        pass

    def read(self, var_index):
        """ handles request to read a variable """

    def write(self, var_index, value):
        """ handles request to write a variable """
        pass

    def commit_var(self, var_index, value):
        """ when a transaction commits, commit the uncommitted variable """
        # if the var is recovering, update status
        pass

    def _acquire_read_lock(self, var_index, transaction_index):
        """ acquire read lock """
        pass

    def _acquire_write_lock(self, var_index, transaction_index):
        """ acquire write lock """
        pass

    def release_all_locks(self, transaction_index):
        """ release all the locks held by a transaction """
        pass

    def dump(self):
        """ dump everything on this site """
        pass
