import os
import logging
import time
from inout import IO
from db_site import Site
from transaction import Transaction
from lock import Lock
from data_manager import DataManager


logging.basicConfig(level=logging.INFO,
                    filename='log.log',
                    filemode='w',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')



NUM_VARS = 20
NUM_SITES = 10

class TransactionManager(object):

    def __init__(self):
        self.global_time = 0
        self.transactions = {}
        self.op_retry_queue = {}
        self.sites = []
        self.sites_fail_time: {int:[]} = {}
        self.wait_for_graph: {int: set()} = {}
        self.lock_waiting_queue: {int: [()]} = {}

        # init sites
        for i in range(1, NUM_SITES+1):
            self.sites.append(Site(i))
        logging.info("TM initialized.")

        #  init lock waiting queue
        for i in range(1, NUM_VARS+1):
            self.lock_waiting_queue[i] = []


    def _tick(self):
        self.global_time += 1


    def execute(self, op=None):
        success = True
        
        # deadlock detection
        self._resolve_deadlock()

        # retry
        for retry_op in list(self.op_retry_queue.keys()):
            retry_success, transaction_index = self.translate_op(retry_op)
            if retry_success:
                self.op_retry_queue.pop(retry_op)


        # call translate to execute op if provided
        if op:
            success, op_transaction_index = self.translate_op(op)

        # retry
        for retry_op in list(self.op_retry_queue.keys()):
            retry_success, transaction_index = self.translate_op(retry_op)
            if retry_success:
                self.op_retry_queue.pop(retry_op)

        # enqueue this op for retrying later if fail
        if not success and self.transactions[op_transaction_index].status != Transaction.TStatus.Aborted:
            self.op_retry_queue[op] = op_transaction_index

        self._tick()

        if op is None and len(self.op_retry_queue) == 0:
            return False
        return True

    def _get_relevent_sites(self, var_index):
        if var_index % 2 == 0:
            return self.sites
        else:
            return [self.sites[var_index % 10]]



    def _resolve_deadlock(self):
        """ if there exist deadlock, resolve it """
        cycle_exist, cycle = self._detect_cycle()
        if cycle_exist:
            self._abort_youngest(cycle)

    def _detect_cycle(self):
        """ determine whether there exists cycles in the wait for graph """

        cycle = []
        cycle_exist = False
        self.cycle_complete = False

        def dfs(G, u, color):
            color[u] = -1
            cycle_start = None
            cycle_exist = False
            if G.get(u) is not None:
                for v in G.get(u):
                    if color[v] == 0:
                        cycle_exist, cycle_start = dfs(G, v, color)
                    elif color[v] == -1:
                        cycle_exist = True
                        cycle_start = v
                        self.cycle_complete = False
                        cycle.append(u)
                        return cycle_exist, cycle_start
            color[u] = 1
            if cycle_exist and not self.cycle_complete:
                cycle.append(u)
                self.cycle_complete = (cycle_start == u)
            return cycle_exist, cycle_start


        color = [0] * (len(self.transactions)+1)
        for u in self.wait_for_graph.keys():
            if color[u] == 0:
                cycle_exist, cycle_start = dfs(self.wait_for_graph, u, color)
                if cycle_exist:
                    break
        
        return cycle_exist, cycle

    def _abort_youngest(self, cycle):
        """ abort the yougest transaction """
        youngest_index = cycle[0]
        for transaction_index in cycle:
            if self.transactions[transaction_index].start_time > self.transactions[youngest_index].start_time:
                youngest_index = transaction_index
        
        logging.info("Aborting T%s to break the deadlock." % transaction_index)
        print("Aborting T%s to break the deadlock." % transaction_index)
        self._abort_transaction(youngest_index)

    
    def translate_op(self, op):
        """ translate an operation """
        if "begin" in op and "beginRO" not in op:
            transaction_index = int(op[op.find("(")+2 : op.find(")")])
            return self._begin(transaction_index), transaction_index
        elif "beginRO" in op:
            transaction_index = int(op[op.find("(")+2 : op.find(")")])
            return self._beginRO(transaction_index), transaction_index
        elif "R" in op:
            op = op.replace(" ", "")
            op = op[op.find("(")+1 : op.find(")")]
            transaction_index = int(op.split(",")[0][1:])
            var_index = int(op.split(",")[1][1:])
            return self._read(transaction_index, var_index), transaction_index
        elif "W" in op:
            op = op.replace(" ", "")
            op = op[op.find("(")+1 : op.find(")")]
            transaction_index = int(op.split(",")[0][1:])
            var_index = int(op.split(",")[1][1:])
            value = int(op.split(",")[2])
            return self._write(transaction_index, var_index, value), transaction_index
        elif "end" in op:
            transaction_index = int(op[op.find("(")+2 : op.find(")")])
            return self._end(transaction_index), transaction_index
        elif "fail" in op:
            site_index = int(op[op.find("(")+1 : op.find(")")])
            return self._fail(site_index), None
        elif "recover" in op:
            site_index = int(op[op.find("(")+1 : op.find(")")])
            return self._recover(site_index), None
        elif "dump" in op:
            return self._dump(), None
        else:
            return True

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
            if T.status == Transaction.TStatus.Aborted:
                logging.info("T%s already aborted." % transaction_index)
                return True
            return self._commit_transaction(transaction_index)
        else:
            # determine whether can commit
            # ensure that all servers you accessed have been up 
            # since the first time they were accessed
            for site in self.sites:
                if site.first_access_time.get(transaction_index) is None:
                    continue
                last_fail_time = -1
                if self.sites_fail_time.get(site.index) is not None:
                    last_fail_index = len(self.sites_fail_time.get(site.index)) - 1
                    last_fail_time = self.sites_fail_time.get(site.index)[last_fail_index]
                if site.first_access_time[transaction_index] < last_fail_time:
                    logging.info("Aborting T%s because some servers it accessed failed after its first access." % transaction_index)
                    print("Aborting T%s because some servers it accessed failed after its first access." % transaction_index)
                    return self._abort_transaction(transaction_index)
            return self._commit_transaction(transaction_index)



    def _commit_transaction(self, transaction_index):
        """ commit a transaction """
        T = self.transactions.get(transaction_index)
        # write uncommitted var values to sites
        for var_index in T.uncommitted_vars.keys():
            for site in self._get_relevent_sites(var_index):
                if site.status != Site.SStatus.Down:
                    site.DM.commit_var(var_index, T.uncommitted_vars[var_index], self.global_time)
        # release all locks
        for site in self.sites:
            if site.status != Site.SStatus.Down:
                site.DM.release_all_locks(transaction_index)
        # check whether lock request in waiting queue can advance
        for var_index in self.lock_waiting_queue.keys():
            waiting_queue = self.lock_waiting_queue.get(var_index)
            if len(waiting_queue) == 0:
                continue
            head_transaction = waiting_queue[0][0]
            head_lock_type = waiting_queue[0][1]
            
            if head_lock_type == Lock.LockType.ReadLock:
                current_locked = True
                for site in self._get_relevent_sites(var_index):
                    if site.status != Site.SStatus.Down and site.DM.get_lock_on_var(var_index) is None:
                        current_locked = False
                        break
                if not current_locked:
                    while head_lock_type == Lock.LockType.ReadLock:
                        for site in self._get_relevent_sites(var_index):
                            success, blocking_transactions = site.DM.acquire_read_lock(var_index, head_transaction)
                            if success:
                                logging.info("Let the first in lock waiting queue (T%s) get read lock on x%s." % (head_lock_type, var_index))
                                self.lock_waiting_queue[var_index].remove((head_transaction, head_lock_type))
                                if len(self.lock_waiting_queue[var_index]) != 0:
                                    head_transaction = self.lock_waiting_queue[var_index][0][0]
                                    head_lock_type = self.lock_waiting_queue[var_index][0][1]
                                break
                        if len(self.lock_waiting_queue[var_index]) == 0:
                            break
                        
            else:
                current_locked = False
                for site in self._get_relevent_sites(var_index):
                    if site.status != Site.SStatus.Down and site.DM.get_lock_on_var(var_index) is not None:
                        if not (site.DM.get_lock_on_var(var_index).lock_type == Lock.LockType.ReadLock and len(site.DM.get_lock_on_var(var_index).transactions) == 1 and site.DM.get_lock_on_var(var_index).transactions[0] == head_transaction):
                            current_locked = True
                            break
                if not current_locked:
                    for site in self._get_relevent_sites(var_index):
                        site.DM.acquire_write_lock(var_index, head_transaction)
                    self.lock_waiting_queue[var_index].remove((head_transaction, head_lock_type))


        # update the wait for graph
        for t in list(self.wait_for_graph.keys()):
            assert(t != transaction_index) # T should not be blocked if it is committing
            if transaction_index in self.wait_for_graph.get(t):
                self.wait_for_graph.get(t).remove(transaction_index)
                if len(self.wait_for_graph.get(t)) == 0:
                    self.wait_for_graph.pop(t)
        # set status
        T.status = Transaction.TStatus.Committed
        logging.info("T%s commits at tick: %s." % (transaction_index, self.global_time))
        print("T%s commits." % transaction_index)
        return True


    def _abort_transaction(self, transaction_index):
        """ abort a transaction """
        T = self.transactions.get(transaction_index)
        # release all locks
        for site in self.sites:
            if site.status != Site.SStatus.Down:
                site.DM.release_all_locks(transaction_index)

        # check whether lock request in waiting queue can advance
        for var_index in self.lock_waiting_queue.keys():
            waiting_queue = self.lock_waiting_queue.get(var_index)
            if len(waiting_queue) == 0:
                continue
            head_transaction = waiting_queue[0][0]
            head_lock_type = waiting_queue[0][1]
            
            if head_lock_type == Lock.LockType.ReadLock:
                current_locked = True
                for site in self._get_relevent_sites(var_index):
                    if site.status != Site.SStatus.Down and site.DM.get_lock_on_var(var_index) is None:
                        current_locked = False
                        break
                if not current_locked:
                    while head_lock_type == Lock.LockType.ReadLock:
                        for site in self._get_relevent_sites(var_index):
                            success, blocking_transactions = site.DM.acquire_read_lock(var_index, head_transaction)
                            if success:
                                self.lock_waiting_queue[var_index].remove((head_transaction, head_lock_type))
                                if len(self.lock_waiting_queue[var_index]) != 0:
                                    head_transaction = self.lock_waiting_queue[var_index][0][0]
                                    head_lock_type = self.lock_waiting_queue[var_index][0][1]
                                break
                        if len(self.lock_waiting_queue[var_index]) == 0:
                            break

            else:
                current_locked = False
                for site in self._get_relevent_sites(var_index):
                    if site.status != Site.SStatus.Down and site.DM.get_lock_on_var(var_index) is not None:
                        if not (site.DM.get_lock_on_var(var_index).lock_type == Lock.LockType.ReadLock and len(site.DM.get_lock_on_var(var_index).transactions) == 1 and site.DM.get_lock_on_var(var_index).transactions[0] == head_transaction):
                            current_locked = True
                            break
                if not current_locked:
                    for site in self._get_relevent_sites(var_index):
                        site.DM.acquire_write_lock(var_index, head_transaction)
                    self.lock_waiting_queue[var_index].remove((head_transaction, head_lock_type))


        # update the wait for graph
        for t in list(self.wait_for_graph.keys()):
            if t == transaction_index:
                self.wait_for_graph.pop(t)
            elif transaction_index in self.wait_for_graph.get(t):
                self.wait_for_graph.get(t).remove(transaction_index)
                if len(self.wait_for_graph.get(t)) == 0:
                    self.wait_for_graph.pop(t)
        # when aborting a transaction, all associated op in retry queue should be removed
        for retry_op in list(self.op_retry_queue.keys()):
            if self.op_retry_queue[retry_op] == transaction_index:
                self.op_retry_queue.pop(retry_op)
        # set status
        T.status = Transaction.TStatus.Aborted
        logging.info("T%s aborts at tick: %s." % (transaction_index, self.global_time))
        print("T%s aborts." % transaction_index)
        return True


    def _read(self, transaction_index, var_index):
        """ read request of a transaction on a variable """
        T = self.transactions.get(transaction_index)
        if T is None or T.status == Transaction.TStatus.Aborted or T.status == Transaction.TStatus.Committed:
            logging.info("Transaction T%s is not active." % transaction_index)
            return True

        if T.read_only:
            return self._read_from_snapshot(transaction_index, var_index, T.start_time)
        else:
            # if lock_waiting_queue for this var is not empty, must be blocked, no need to try read in DM
            if len(self.lock_waiting_queue[var_index]) != 0:
                # have this transaction acquired lock
                acquired_lock = False
                for site in self._get_relevent_sites(var_index):
                    lock_on_var = site.DM.get_lock_on_var(var_index)
                    if lock_on_var is not None and ((lock_on_var.LockType == Lock.LockType.ReadLock and transaction_index in lock_on_var.transactions) or (lock_on_var.lock_type == Lock.LockType.WriteLock and transaction_index in lock_on_var.transactions)):
                        acquired_lock = True
                        break
                if not acquired_lock:
                    # check whether this transaction is in the lock waiting queue
                    existing_transactions = []
                    for wait in self.lock_waiting_queue[var_index]:
                        existing_transactions.append(wait[0])
                    if transaction_index in existing_transactions:
                        return False

                    # update wait for graph
                    last_in_queue = self.lock_waiting_queue[var_index][len(self.lock_waiting_queue[var_index]) - 1]
                    if last_in_queue[1] == Lock.LockType.WriteLock:
                        if self.wait_for_graph.get(transaction_index) is None:
                            self.wait_for_graph[transaction_index] = set()
                        self.wait_for_graph[transaction_index].update([last_in_queue[0]])
                    else:
                        last_in_queue_wait = self.wait_for_graph.get(last_in_queue[0], [])
                        if self.wait_for_graph.get(transaction_index) is None:
                            self.wait_for_graph[transaction_index] = set()
                        self.wait_for_graph[transaction_index] = set(last_in_queue_wait)

                    # update lock waiting queue
                    self.lock_waiting_queue[var_index].append((transaction_index, Lock.LockType.ReadLock))
                    logging.info("Other ops waiting for lock on x%s. T%s has to wait for read lock in the queue." % (var_index, transaction_index))
                    return False


            
            # first check uncommitted var
            uncommitted = self.transactions[transaction_index].uncommitted_vars.get(var_index)
            if uncommitted is not None:
                IO.print_var(var_index, uncommitted)
                logging.info("Read x%s = %s from uncommitted variables in T%s." % (var_index, uncommitted, transaction_index))
                return True

            relevent_sites = self._get_relevent_sites(var_index)
            num_sites_unavailable = 0
            for site in relevent_sites:
                if site.status == Site.SStatus.Down:
                    num_sites_unavailable += 1
                    continue

                success, blocking_transactions = site.DM.read(var_index, transaction_index)
                if not success and len(blocking_transactions) > 0: # waiting for lock
                    if self.wait_for_graph.get(transaction_index) is None:
                        self.wait_for_graph[transaction_index] = set()
                    self.wait_for_graph[transaction_index].update(blocking_transactions)
                    self.transactions[transaction_index].status = Transaction.TStatus.Blocked
                    # update lock waiting queue
                    self.lock_waiting_queue[var_index].append((transaction_index, Lock.LockType.ReadLock))
                    return False
                elif not success and len(blocking_transactions) == 0: # variable not ready
                    num_sites_unavailable += 1
                elif success:
                    # record first access time
                    if site.first_access_time.get(transaction_index) == None:
                        site.first_access_time[transaction_index] = self.global_time
                    break
            if num_sites_unavailable == len(relevent_sites): # no sites available for read
                return False

            return success


            
    def _write(self, transaction_index, var_index, value):
        """ write request of a transaction on a variable """
        T = self.transactions.get(transaction_index)
        if T is None or T.status == Transaction.TStatus.Aborted or T.status == Transaction.TStatus.Committed:
            logging.info("Transaction T%s is not active." % transaction_index)
            return True

        relevent_sites = self._get_relevent_sites(var_index)
        num_sites_unavailable = 0

        # if lock_waiting_queue for this var is not empty, must be blocked, no need to try read in DM
        if len(self.lock_waiting_queue[var_index]) != 0:
            # have this transaction acquired lock
            acquired_lock = False
            for site in self._get_relevent_sites(var_index):
                lock_on_var = site.DM.get_lock_on_var(var_index)
                if lock_on_var is not None and lock_on_var.lock_type == Lock.LockType.WriteLock and transaction_index in lock_on_var.transactions:
                    acquired_lock = True
                    break
            if not acquired_lock:
                # check whether this transaction is in the lock waiting queue
                existing_transactions = []
                for wait in self.lock_waiting_queue[var_index]:
                    existing_transactions.append(wait[0])
                if transaction_index in existing_transactions:
                    return False

                # update wait for graph
                len_waiting_queue = len(self.lock_waiting_queue[var_index])
                last_in_queue = self.lock_waiting_queue[var_index][len_waiting_queue - 1]
                if last_in_queue[1] == Lock.LockType.WriteLock:
                    if self.wait_for_graph.get(transaction_index) is None:
                        self.wait_for_graph[transaction_index] = set()
                    self.wait_for_graph[transaction_index].update([last_in_queue[0]])
                else:
                    preceding_read_transactions = []
                    for i in range(len_waiting_queue - 1, -1, -1):
                        if self.lock_waiting_queue[var_index][i][1] == Lock.LockType.ReadLock:
                            if self.lock_waiting_queue[var_index][i][0] != transaction_index:
                                preceding_read_transactions.append(self.lock_waiting_queue[var_index][i][0])
                        else:
                            break
                    if self.wait_for_graph.get(transaction_index) is None:
                        self.wait_for_graph[transaction_index] = set()
                    self.wait_for_graph[transaction_index].update(preceding_read_transactions)

                # update lock waiting queue
                self.lock_waiting_queue[var_index].append((transaction_index, Lock.LockType.WriteLock))
                logging.info("Other ops waiting for lock on x%s. T%s has to wait for write lock in the queue." % (var_index, transaction_index))
                return False

        
        # try lock on all sites
        can_lock = True
        blocking_transactions = set()
        for site in relevent_sites:
            if site.status == Site.SStatus.Down:
                num_sites_unavailable += 1
                continue
            can_lock_on_site, blocking_transactions_on_site = site.DM.try_write_lock(var_index, transaction_index)
            if not can_lock_on_site:
                can_lock = False
                blocking_transactions.update(blocking_transactions_on_site)
        if transaction_index in blocking_transactions:
            blocking_transactions.remove(transaction_index)
        # update wait for graph
        if not can_lock:
            if self.wait_for_graph.get(transaction_index) is None:
                self.wait_for_graph[transaction_index] = set()
            self.wait_for_graph[transaction_index].update(blocking_transactions)
            self.transactions[transaction_index].status = Transaction.TStatus.Blocked
            # update lock waiting queue
            self.lock_waiting_queue[var_index].append((transaction_index, Lock.LockType.WriteLock))
            return False
                
        if num_sites_unavailable == len(relevent_sites):
            return False


        for site in relevent_sites:
            if site.status == Site.SStatus.Down:
                continue
            success, blocking_transactions = site.DM.write(var_index, value, transaction_index)
            # record first access time
            if site.first_access_time.get(transaction_index) == None:
                site.first_access_time[transaction_index] = self.global_time
            
        
        # if can write, save value in uncommitted vars
        self.transactions[transaction_index].write_uncommitted(var_index, value)

        return True


    def _fail(self, site_index):
        """ make a site fail """
        self.sites[site_index - 1].fail(self.global_time)
        if self.sites_fail_time.get(site_index) is None:
            self.sites_fail_time[site_index] = []
        self.sites_fail_time[site_index].append(self.global_time)
        return True

    def _recover(self, site_index):
        """ make a site recover """
        self.sites[site_index - 1].recover()
        return True

    def _dump(self):
        """ dump committed values of all copies of all variables at all sites """
        snapshot = {}
        for site_index, site in enumerate(self.sites):
            site_snapshot = site.DM.dump()
            snapshot[site_index+1] = site_snapshot
        IO.dump(snapshot)
        return True


    def _read_from_snapshot(self, transaction_index, var_index, start_time):
        """ read for RO transactions """
        success = False
        retry = False

        if var_index % 2 != 0:
            # odd indexed (no duplicates)
            site = self.sites[var_index % 10]
            if site.status != Site.SStatus.Down:
                success = site.DM.read_from_snapshot(var_index, start_time, None, None, transaction_index)
            else:
                retry = True
        else:
            # even indexed (duplicates)
            relevent_sites = self._get_relevent_sites(var_index)
            num_sites_down = 0
            for site in relevent_sites:
                if site.status == Site.SStatus.Down:
                    num_sites_down += 1
                    continue
                last_fail_time = None
                first_fail_time = None
                if self.sites_fail_time.get(site.index) is not None:
                    last_fail_index = len(self.sites_fail_time.get(site.index)) - 1
                    last_fail_time = self.sites_fail_time.get(site.index)[last_fail_index]
                    first_fail_time = self.sites_fail_time.get(site.index)[0]
                success = site.DM.read_from_snapshot(var_index, start_time, first_fail_time, last_fail_time, transaction_index)
                if success:
                    break
            if num_sites_down == len(relevent_sites):
                retry = True
        if not success and not retry:
            logging.info("Aborting T%s because no relevent site has a committed version before T%s began and has not failed in between." % (transaction_index, transaction_index))
            print("Aborting T%s because no relevent site has a committed version before T%s began and has not failed in between." % (transaction_index, transaction_index))
            self._abort_transaction(transaction_index)
        return success


