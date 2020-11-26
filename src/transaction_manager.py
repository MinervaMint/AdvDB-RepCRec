import os
import logging
import time
from .io import IO
from .site import Site
from .transaction import Transaction

NUM_VARS = 20
NUM_SITES = 10

class TransactionManager(object):

    def __init__(self):
        self.global_time = 0
        self.transactions = {}
        self.op_retry_queue = []
        self.sites = []
        self.wait_for_graph: {int: set()} = {}

        # init sites
        for i in range(1, NUM_SITES+1):
            self.sites.append(Site(i))


    def _tick(self):
        self.global_time += 1


    def execute(self, op=None):
        success = True # is this op successfully executed, do we need to put it in op_retry_queue

        # deadlock detection
        self._resolve_deadlock()

        # call translate to execute op if provided
        if op:
            success = self.transalate_op(op)

        # retry
        for retry_op in self.op_retry_queue:
            retry_success = self.transalate_op(retry_op)
            if retry_success:
                self.op_retry_queue.remove(retry_op)

        # enqueue this op for retrying later if fail
        if not success:
            self.op_retry_queue.append(op)

        # TODO: when should we tick?
        self._tick()




    def _resolve_deadlock(self):
        """ if there exist deadlock, resolve it """
        cycle_exist, cycle = self._detect_cycle()
        if cycle_exist:
            self._abort_youngest(cycle)

    def _detect_cycle(self):
        """ determine whether there exists cycles in the wait for graph """

        def dfs(G, u, color):
            cycle_start = None
            color[u] = -1
            cycle_exist = False
            for v in G.get(u):
                if color[v] == 0:
                    cycle_exist = dfs(G, v, color)
                elif color[v] == -1:
                    cycle_exist = True
                    cycle_start = v
                    return cycle_exist, cycle_start
            color[u] = 1
            return cycle_exist, cycle_start

        cycle = []
        cycle_exist = False
        cycle_start = None

        color = [0] * len(self.transactions)
        for u in self.wait_for_graph.keys():
            if color[u] == 0:
                cycle_exist, cycle_start = dfs(self.wait_for_graph, u, color)
                if cycle_exist:
                    break
        
        if not cycle_exist:
            return cycle_exist, cycle

        u = cycle_start
        cycle.append(u)
        cycle_complete = False
        while not cycle_complete:
            for v in self.wait_for_graph.get(u):
                if v == cycle_start:
                    cycle_complete = True
                    break
                if color[v] == -1:
                    cycle.append(v)
                    u = v
                    break

        return cycle_exist, cycle

    def _abort_youngest(self, cycle):
        """ abort the yougest transaction """
        youngest_index = cycle[0]
        for transaction_index in cycle:
            if self.transactions[transaction_index].start_time < self.transactions[youngest_index].start_time:
                youngest_index = transaction_index
        
        self._abort_transaction(youngest_index)

    
    def transalate_op(self, op):
        """ translate an operation """
        if "begin" in op and "beginRO" not in op:
            transaction_index = op[op.find("(")+2 : op.find(")")]
            return self._begin(transaction_index)
        elif "beginRO" in op:
            transaction_index = op[op.find("(")+2 : op.find(")")]
            return self._beginRO(transaction_index)
        elif "R" in op:
            op = op.replace(" ", "")
            op = op[op.find("(")+1 : op.find(")")]
            transaction_index = int(op.split(",")[0][1:])
            var_index = int(op.split(",")[1][1:])
            return self._read(transaction_index, var_index)
        elif "W" in op:
            op = op.replace(" ", "")
            op = op[op.find("(")+1 : op.find(")")]
            transaction_index = int(op.split(",")[0][1:])
            var_index = int(op.split(",")[1][1:])
            value = int(op.split(",")[2])
            return self._write(transaction_index, var_index, value)
        elif "end" in op:
            transaction_index = op[op.find("(")+2 : op.find(")")]
            return self._end(transaction_index)
        elif "fail" in op:
            site_index = int(op[op.find("(")+1 : op.find(")")])
            return self._fail(site_index)
        elif "recover" in op:
            site_index = int(op[op.find("(")+1 : op.find(")")])
            return self._recover(site_index)
        elif "dump" in op:
            return self._dump()

    def _begin(self, transaction_index):
        """ start a not read-only transaction """
        T = Transaction(transaction_index, False, self.global_time)
        self.transactions[transaction_index] = T
        return True

    def _beginRO(self, transaction_index):
        """ start a read-only transaction """
        T = Transaction(transaction_index, True, self.global_time)
        self.transactions[transaction_index] = T
        return True


    def _end(self, transaction_index):
        """ end a transaction """
        T = self.transactions.get(transaction_index)
        # if already aborted?
        if T.status == Transaction.TStatus.Aborted:
            logging.info("Transaction T%s is already aborted." % transaction_index)
            return True

        if T.read_only:
            # read only transactions always commits
            T.status = Transaction.TStatus.Committed
            return True
        else:
            # determine whether can commit
            # ensure that all servers you accessed have been up 
            # since the first time they were accessed
            for site in self.sites:
                if site.first_access_time.get(transaction_index) is None:
                    continue
                if site.first_access_time[transaction_index] < site.last_fail_time:
                    return self._abort_transaction(transaction_index)
            return self._commit_transaction(transaction_index)



    def _commit_transaction(self, transaction_index):
        """ commit a transaction """
        T = self.transactions.get(transaction_index)
        # write uncommitted var values to sites
        for var_index in T.uncommitted_vars.keys():
            for site in self.sites:
                if site.status != Site.SStatus.Down:
                    site.DM.commit_var(var_index, T.uncommitted_vars[var_index])
        # release all locks
        for site in self.sites:
            if site.status != Site.SStatus.Down:
                site.DM.release_all_locks(transaction_index)
        # update the wait for graph
        for t in self.wait_for_graph.keys():
            assert(t != transaction_index) # T should not be blocked if it is committing
            if transaction_index in self.wait_for_graph.get(t):
                self.wait_for_graph.get(t).remove(transaction_index)
                if len(self.wait_for_graph.get(t)) == 0:
                    self.wait_for_graph.pop(t)
        # set status
        T.status = Transaction.TStatus.Committed
        return True


    def _abort_transaction(self, transaction_index):
        """ abort a transaction """
        T = self.transactions.get(transaction_index)
        # release all locks
        for site in self.sites:
            if site.status != Site.SStatus.Down:
                site.DM.release_all_locks(transaction_index)
        # update the wait for graph
        for t in self.wait_for_graph.keys():
            if t == transaction_index:
                self.wait_for_graph.pop(t)
            elif transaction_index in self.wait_for_graph.get(t):
                self.wait_for_graph.get(t).remove(transaction_index)
                if len(self.wait_for_graph.get(t)) == 0:
                    self.wait_for_graph.pop(t)
        # set status
        T.status = Transaction.TStatus.Aborted
        return True


    def _read(self, transaction_index, var_index):
        """ read request of a transaction on a variable """
        T = self.transactions.get(transaction_index)
        if T is None or T.status != Transaction.TStatus.Running:
            logging.info("Transaction T%s is not active." % transaction_index)
            return False

        if T.read_only:
            # TODO: rewrite read from snapshot
            value = T.snapshot.get(var_index)
            if value is not None:
                IO.print_var(var_index, value)
            else:
                return False
        else:
            if var_index % 2 == 0:
                num_sites_down = 0
                for site in self.sites:
                    if site.status == Site.SStatus.Down:
                        num_sites_down += 1
                        continue
                    # TODO: how to read from only one available site but update locks in all
                    success, blocking_transactions = site.DM.read(var_index, transaction_index)
                    if not success:
                        self.wait_for_graph[transaction_index].update(blocking_transactions)
                        return False
                if num_sites_down == NUM_SITES:
                    return False

                # record first access time
                for site in self.sites:
                    if site.first_access_time.get(transaction_index) == None:
                        site.first_access_time[transaction_index] = self.global_time
                return True

            else:
                site = self.sites[var_index % 10]
                if site.status == Site.SStatus.Down:
                    return False
                success, blocking_transactions = site.DM.read(var_index, transaction_index)
                if not success:
                    self.wait_for_graph[transaction_index].update(blocking_transactions)
                    return False

                if site.first_access_time.get(transaction_index) == None:
                    site.first_access_time[transaction_index] = self.global_time
                return True

            
    def _write(self, transaction_index, var_index, value):
        """ write request of a transaction on a variable """
        T = self.transactions.get(transaction_index)
        if T is None or T.status != Transaction.TStatus.Running:
            logging.info("Transaction T%s is not active." % transaction_index)
            return False
        
        if var_index % 2 == 0:
            num_sites_down = 0
            for site in self.sites:
                if site.status == Site.SStatus.Down:
                    num_sites_down += 1
                    continue
                success, blocking_transactions = site.DM.write(var_index, value, transaction_index)
                if not success:
                    self.wait_for_graph[transaction_index].update(blocking_transactions)
                    return False

            if num_sites_down == NUM_SITES:
                return False
            # if can write, save value in uncommitted vars
            self.transactions[transaction_index].write_uncommitted(var_index, value)
            # record first access time
            for site in self.sites:
                if site.first_access_time.get(transaction_index) == None:
                    site.first_access_time[transaction_index] = self.global_time
            return True
        else:
            site = self.sites[var_index % 10]
            if site.status == Site.SStatus.Down:
                return False
            success, blocking_transactions = site.DM.write(var_index, value, transaction_index)
            if not success:
                self.wait_for_graph[transaction_index].update(blocking_transactions)
                return False

            self.transactions[transaction_index].write_uncommitted(var_index, value)
            if site.first_access_time.get(transaction_index) == None:
                site.first_access_time[transaction_index] = self.global_time
            return True


    def _fail(self, site_index):
        """ make a site fail """
        self.sites[site_index].fail(self.global_time)
        return True

    def _recover(self, site_index):
        """ make a site recover """
        self.sites[site_index].recover()
        return True

    def _dump(self):
        """ dump committed values of all copies of all variables at all sites """
        snapshot = {}
        for site_index, site in enumerate(self.sites):
            site_snapshot = site.DM.dump()
            snapshot[site_index+1] = site_snapshot
        return snapshot


    # TODO: replace
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
