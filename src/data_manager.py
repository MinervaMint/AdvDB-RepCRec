import os
import logging
from enum import Enum
from lock import Lock
from inout import IO

NUM_VARS = 20

class DataManager(object):

    VStatus = Enum("VStatus", ("Ready", "Unavailable", "Recovering"))

    def __init__(self, associated_site):
        self.associated_site = associated_site
        self.variables: {int: [()]} = {}
        self.variable_status = {}
        self.locktable = {}

        # init variables and variable status
        for i in range(1, NUM_VARS+1):
            if i % 2 == 0 or i % 10 + 1 == associated_site:
                self.variables[i] = []
                self.variables[i].append((0, i * 10))
                self.variable_status[i] = self.VStatus.Ready



    def fail(self):
        """ when the corresponding site fails """
        # lock information may be lost
        self.locktable = {}
        # change variable status
        for var_index in self.variable_status.keys():
            self.variable_status[var_index] = self.VStatus.Unavailable
        logging.info("Site %s fails." % (self.associated_site))
        

    def recover(self):
        """ when the corresponding site recovers """
        # change variable status
        for var_index in self.variable_status.keys():
            if var_index % 2 == 0:
                self.variable_status[var_index] = self.VStatus.Recovering
            else:
                self.variable_status[var_index] = self.VStatus.Ready
        logging.info("Site %s recovers." % (self.associated_site))


    def get_committed_var(self, var_index):
        """ get latest committed value of a variable """
        # if a var is ready return its value
        if self.variable_status.get(var_index) == self.VStatus.Ready:
            num_versions = len(self.variables.get(var_index))
            return self.variables.get(var_index)[num_versions - 1][1]
        return None


    def read(self, var_index, transaction_index):
        """ handles request to read a variable """
        # see if variable status ready (what if recovering?)
        if self.variable_status.get(var_index) != self.VStatus.Ready:
            return False, []

        # try to acquire read lock
        # if obtained lock, read
        can_lock, blocking_transactions = self.acquire_read_lock(var_index, transaction_index)
        if can_lock:
            logging.info("T%s acquired read lock on x%s." % (transaction_index, var_index))
            value = self.get_committed_var(var_index)
            logging.info("Read x%s = %s from site %s by T%s." % (var_index, value, self.associated_site, transaction_index))
            IO.print_var(var_index, value)
            return True, []

        # if cannot obtain lock
        logging.info("Cannot acquire read lock on x%s for T%s." % (var_index, transaction_index))
        return can_lock, blocking_transactions


        

    def write(self, var_index, value, transaction_index):
        """ handles request to write a variable """
        assert(self.variable_status.get(var_index) != self.VStatus.Unavailable)
        # try to acquire write lock
        # if obtained lock, write (write value in transaction's uncommitted vars)
        success, blocking_transactions = self.acquire_write_lock(var_index, transaction_index)
        if success:
            logging.info("T%s acquired write lock on x%s." % (transaction_index, var_index))
        return success, blocking_transactions




    def commit_var(self, var_index, value, tick):
        """ when a transaction commits, commit the uncommitted variable, record it as a new version """
        # update value in variables
        self.variables[var_index].append((tick, value))
        logging.info("Commit x%s = %s to site %s at tick: %s." % (var_index, value, self.associated_site, tick))
        # if the var is recovering, update status
        if self.variable_status.get(var_index) == self.VStatus.Recovering:
            self.variable_status[var_index] = self.VStatus.Ready
        

    def acquire_read_lock(self, var_index, transaction_index):
        """ acquire read lock """
        # check lock table
        current_lock = self.locktable.get(var_index)
        if current_lock == None:
            new_lock = Lock(Lock.LockType.ReadLock)
            new_lock.transactions.append(transaction_index)
            self.locktable[var_index] = new_lock
            return True, []
        elif current_lock.lock_type == Lock.LockType.ReadLock:
            if transaction_index not in current_lock.transactions:
                self.locktable[var_index].transactions.append(transaction_index)
            return True, []
        elif current_lock.lock_type == Lock.LockType.WriteLock:
            if current_lock.transactions[0] == transaction_index:
                logging.info("T%s already has write lock on x%s." % (transaction_index, var_index))
                return True, []
            blocking_transactions = current_lock.transactions
            return False, blocking_transactions


        

    def acquire_write_lock(self, var_index, transaction_index):
        """ acquire write lock """
        # check lock table
        current_lock = self.locktable.get(var_index)
        if current_lock is None:
            new_lock = Lock(Lock.LockType.WriteLock)
            new_lock.transactions.append(transaction_index)
            self.locktable[var_index] = new_lock
            return True, []
        elif current_lock.lock_type == Lock.LockType.ReadLock and len(current_lock.transactions) == 1 and current_lock.transactions[0] == transaction_index:
            self.locktable[var_index].lock_type = Lock.LockType.WriteLock
            logging.info("Promoting T%s's read lock on x%s to write lock." % (transaction_index, var_index))
            return True, []
        elif current_lock.lock_type == Lock.LockType.WriteLock and current_lock.transactions[0] == transaction_index:
            logging.info("T%s already has write lock on x%s." % (transaction_index, var_index))
            return True, []
        else:
            blocking_transactions = current_lock.transactions
            return False, blocking_transactions


    def try_write_lock(self, var_index, transaction_index):
        """ return whether a transaction can acquire write lock on a var (do not actually lock) """
        current_lock = self.locktable.get(var_index)
        if current_lock is None:
            return True, []
        if current_lock.lock_type == Lock.LockType.ReadLock and len(current_lock.transactions) == 1 and current_lock.transactions[0] == transaction_index:
            return True, []
        if current_lock.lock_type == Lock.LockType.WriteLock and current_lock.transactions[0] == transaction_index:
            return True, []
        blocking_transactions = current_lock.transactions
        logging.info("Cannot acquire write lock on x%s for T%s." % (var_index, transaction_index))
        logging.debug("Blocked by transaction(s): ")
        logging.debug(blocking_transactions)
        return False, blocking_transactions

        

    def release_all_locks(self, transaction_index):
        """ release all the locks held by a transaction """
        for var in list(self.locktable.keys()):
            lock = self.locktable.get(var)
            if lock is None:
                continue
            if transaction_index in lock.transactions:
                self.locktable[var].transactions.remove(transaction_index)
                lock_type = "read" if lock.lock_type == Lock.LockType.ReadLock else "write"
                logging.info("Released T%s's %s lock on x%s on site %s." % (transaction_index, lock_type, var, self.associated_site))
                if len(self.locktable[var].transactions) == 0:
                    self.locktable.pop(var)
              

    def get_lock_on_var(self, var_index):
        """ return the current lock on a var """
        return self.locktable.get(var_index)



    def dump(self):
        """ dump current variables on this site """
        snapshot = {}
        for var_index in self.variables.keys():
            num_versions = len(self.variables.get(var_index))
            snapshot[var_index] = self.variables.get(var_index)[num_versions - 1][1]
        return snapshot


    def read_from_snapshot(self, var_index, start_time, first_fail_time, last_fail_time, transaction_index):
        """ multiversion read for RO transactions """
        success = False
        var_versions = self.variables.get(var_index)
        num_versions = len(var_versions)
        for version in range(num_versions-1, -1, -1):
            tick = var_versions[version][0]
            if tick <= start_time:
                if first_fail_time is None or (first_fail_time > start_time) or last_fail_time < tick:
                    success = True
                    value = var_versions[version][1]
                    IO.print_var(var_index, value)
                    logging.info("Read x%s = %s from site %s by T%s." % (var_index, value, self.associated_site, transaction_index))
                break
        return success
