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
        # lock information may be lost

        # change variable status
        for var in self.variable_status.keys():
            self.variable_status[var] = VStatus.Unavailable
        

    def recover(self):
        """ when the corresponding site recovers """
        # change variable status
        for var in self.variable_status.keys():
            self.variable_status[var] = VStatus.Recovering


    def get_committed_var(self, var_index):
        """ get committed value of a variable """
        # if a var is ready return its value
        if self.variable_status.get(var_index) == VStatus.Ready:
            return self.variables.get(var_index)
        return None


    def read(self, var_index, transaction_index):
        """ handles request to read a variable """
        # see if variable status ready (what if recovering?)
        if self.variable_status.get(var_index) != VStatus.Ready:
            return None

        # try to acquire read lock
        # if obtained lock, read
        if self._acquire_read_lock(var_index, transaction_index):
            return self.variables.get(var_index)

        # if cannot obtain lock
        return None


        

    def write(self, var_index, value, transaction_index):
        """ handles request to write a variable """
        assert(self.variable_status.get(var_index) != VStatus.Unavailable)
        # try to acquire write lock
        # if obtained lock, write (write value in transaction's uncommitted vars?)
        if self._acquire_write_lock(var_index, transaction_index):
            return True
        return False




    def commit_var(self, var_index, value):
        """ when a transaction commits, commit the uncommitted variable """
        # update value in variables
        self.variables[var_index] = value
        # if the var is recovering, update status
        if self.variable_status.get(var_index) == VStatus.Recovering:
            self.variable_status[var_index] = VStatus.Ready
        

    def _acquire_read_lock(self, var_index, transaction_index):
        """ acquire read lock """
        # check lock table
        current_lock = self.locktable.get(var_index)
        if current_lock == None:
            new_lock = Lock(Lock.LockType.ReadLock)
            new_lock.transactions.append(transaction_index)
            self.locktable[var_index] = new_lock
            return True
        elif current_lock.lock_type == Lock.LockType.ReadLock:
            self.locktable[var_index].transactions.append(transaction_index)
            return True
        elif current_lock.lock_type == Lock.LockType.WriteLock:
            return False


        

    def _acquire_write_lock(self, var_index, transaction_index):
        """ acquire write lock """
        # check lock table
        current_lock = self.locktable.get(var_index)
        if current_lock == None:
            new_lock = Lock(Lock.LockType.WriteLock)
            new_lock.transactions.append(transaction_index)
            self.locktable[var_index] = new_lock
            return True
        else:
            return False

        

    def release_all_locks(self, transaction_index):
        """ release all the locks held by a transaction """
        for var in self.locktable.keys():
            lock = self.locktable.get(var)
            if lock == None:
                continue
            if transaction_index in lock.transactions:
                self.locktable[var].transactions.remove(transaction_index)
                if len(self.locktable[var].transactions) == 0:
                    self.locktable.pop(var)



    def dump(self):
        """ dump everything on this site """
        return self.variables
