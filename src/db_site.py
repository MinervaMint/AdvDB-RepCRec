import os
import logging
from enum import Enum
from data_manager import DataManager

class Site(object):

    SStatus = Enum("SStatus", ('Up','Down', 'Recovering'))

    def __init__(self, index):
        self.index = index
        self.status = self.SStatus.Up
        self.DM = DataManager(index)
        self.last_fail_time = -1
        self.first_access_time = {}

    def fail(self, tick):
        """ fail this site """
        self.DM.fail()
        self.last_fail_time = tick
        self.status = self.SStatus.Down

    def recover(self):
        """ recover this site """
        self.DM.recover()
        self.status = self.SStatus.Recovering
