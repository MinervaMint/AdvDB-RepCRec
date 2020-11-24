import os
import logging
import time
from .io import IO
from .site import Site
from .transaction import Transaction

NUM_VARS = 20

class TransactionManager(object):

    def __init__(self):
        self.global_time = 0
        self.active_transactions = {}
        self.blocked_transactions = {}
        self.sites = []

        # TODO: data for cycle detection

        # TODO: init sites


    def tick(self):
        self.global_time = self.global_time + 1

        # TODO: deadlock detection

        # TODO: retry?

    
    def transalate_op(self, op):
        """ translate an operation """
        if "begin" in op and "beginRO" not in op:
            transaction_name = op[op.find("(")+1 : op.find(")")]
            self._begin(transaction_name)
        elif "beginRO" in op:
            transaction_name = op[op.find("(")+1 : op.find(")")]
            self._beginRO(transaction_name)
        elif "R" in op:
            op = op.replace(" ", "")
            op = op[op.find("(")+1 : op.find(")")]
            transaction_name = op.split(",")[0]
            var_name = op.split(",")[1]
            self._read(transaction_name, var_name)
        elif "W" in op:
            op = op.replace(" ", "")
            op = op[op.find("(")+1 : op.find(")")]
            transaction_name = op.split(",")[0]
            var_name = op.split(",")[1]
            value = int(op.split(",")[2])
            self._write(transaction_name, var_name, value)
        elif "end" in op:
            transaction_name = op[op.find("(")+1 : op.find(")")]
            self._end(transaction_name)
        elif "fail" in op:
            site_index = int(op[op.find("(")+1 : op.find(")")])
            self._fail(site_index)
        elif "recover" in op:
            site_index = int(op[op.find("(")+1 : op.find(")")])
            self._recover(site_index)
        elif "dump" in op:
            self._dump()

    def _begin(self, transaction_name):
        """ start a not read-only transaction """
        T = Transaction(transaction_name, False, self.global_time)
        self.active_transactions[transaction_name] = T

    def _beginRO(self, transaction_name):
        """ start a read-only transaction """
        T = Transaction(transaction_name, True, self.global_time)
        # TODO: take snapshot of all committed vars
        T.snapshot = self._take_snapshot()
        self.active_transactions[transaction_name] = T

    def _end(self, transaction_name):
        """ end a transaction """
        T = self.active_transactions.get(transaction_name)
        # TODO: if already aborted?
        if T.read_only:
            # read only transactions always commits
            T.status = Transaction.TStatus.Committed
            self.active_transactions.pop(transaction_name)
        else:
            # determine whether can commit
            # ensure that all servers you accessed have been up 
            # since the first time they were accessed
            for site in self.sites:
                if site.first_access_time.get(transaction_name) is None:
                    continue
                if site.first_access_time[transaction_name] < site.last_fail_time or site.status == Site.SStatus.Down:
                    self._abort_transaction(transaction_name)
                    self.active_transactions.pop(transaction_name)
                    return
            self._commit_transaction(transaction_name)

    def _commit_transaction(self, transaction_name):
        """ commit a transaction """
        # write uncommitted var values to sites
        T = self.active_transactions.get(transaction_name)
        for var_index in T.uncommitted_vars.keys():
            for site in self.sites:
                if site.status != Site.SStatus.Down:
                    site.DM.commit_var(var_index, T.uncommitted_vars[var_index])
        # release all locks
        for site in self.sites:
            if site.status == Site.SStatus.Down:
                continue
            site.DM.release_all_locks(transaction_name)
        # set status
        T.status = Transaction.TStatus.Committed
        self.active_transactions.pop(transaction_name)

    def _abort_transaction(self, transaction_index):
        """ abort a transaction """
        pass


    def _read(self, transaction_name, var_name):
        """ read request of a transaction on a variable """
        T = self.active_transactions.get(transaction_name)
        if T is None:
            logging.info("Transaction %s is not active." % transaction_name)
            return
        var_index = int(var_name[1:])
        if T.read_only:
            value = T.snapshot.get(var_index)
            if value is not None:
                IO.print_var(var_name, value)
            else:
                return
        else:
            site_index = None
            if var_index % 2 == 0:
                for i in range(1, len(self.sites)+1):
                    if self.sites[i-1].status == Site.SStatus.Up:
                        site_index = i - 1
                        break
            else:
                site_index = var_index % 10
            
            if site_index == None:
                return
            self.sites[site_index].DM.read(var_index)
            # TODO: record first access time
                
            


    def _write(self, transaction_name, var_name, value):
        """ write request of a transaction on a variable """
        T = self.active_transactions.get(transaction_name)
        if T is None:
            logging.info("Transaction %s is not active." % transaction_name)
            return
        var_index = int(var_name[1:])
        
        num_sites_down = 0
        for site in self.sites:
            if site.status == Site.SStatus.Down:
                num_sites_down += 1
                continue
            site.DM.write(var_index, value)
            # TODO: record first access time
        if num_sites_down == len(self.sites):
            # TODO: raise error?
            return




    def _fail(self, site_index):
        """ make a site fail """
        self.sites[site_index].fail(self.global_time)

    def _recover(self, site_index):
        """ make a site recover """
        self.sites[site_index].recover()

    def _dump(self):
        """ dump committed values of all copies of all variables at all sites """
        snapshot = {}
        for site_index, site in enumerate(self.sites):
            site_snapshot = site.DM.dump()
            snapshot[site_index+1] = site_snapshot
        return snapshot

    def _take_snapshot(self):
        """ take a snapshot of committed var """
        snapshot = {}
        for i in range(1, NUM_VARS+1):
            if i % 2 == 0: # even indexed var at all sites
                for site in self.sites:
                    if site.status == Site.SStatus.Up:
                        snapshot[i] = site.DM.get_committed_var(i)
                        break
            else: # odd indexed var at one site
                site = self.sites[i % 10]
                snapshot[i] = site.DM.get_committed_var(i)
            # TODO: if no committed value?
            if snapshot[i] == None:
                return None
        return snapshot
