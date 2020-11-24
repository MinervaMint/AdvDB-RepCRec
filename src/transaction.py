import os
import logging
from enum import Enum

class Transaction(object):

    TStatus = Enum("TStatus", ('Running', 'Blocked', 'Committed', 'Aborted'))

    def __init__(self, name, read_only, start_time):
        self.name = name
        self.uncommitted_vars = {}
        self.status = TStatus.Running
        self.read_only = read_only
        self.start_time = start_time
        self.snapshot = {}

    
