import os
import logging
from .locktable import LockTable

class DataManager(object):

    def __init__(self, associated_site):
        self.associated_site = associated_site
        self.variables = {}
        self.variable_status = {}
        self.locktable = {}

        # TODO: init variables

    def fail(self):
        """ when the corresponding site fails """
        pass

    def recover(self):
        """ when the corresponding site recovers """
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
