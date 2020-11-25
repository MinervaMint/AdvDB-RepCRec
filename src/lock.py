import os
import logging
from enum import Enum

class Lock(object):

    LockType = Enum("LockType", ('ReadLock', 'WriteLock'))

    def __init__(self, lock_type):
        self.lock_type = lock_type
        self.transactions = [] # only read lock may have multiple associated transactions