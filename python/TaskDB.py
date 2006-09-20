from WorkSpace import WorkSpace
from crab_exceptions import *
import common

import os, string

"""
Class to represent a task dedicated DB, suited to host all info which are
common to a given task. It is implemented in term of a persistent dictionary
"""

class TaskDB:
    def __init__(self):
        self.fName = 'task'
        self._theDict = {}

    def load(self):
        """
        load the dictionary into memory
        """
        fl = open(common.work_space.shareDir() + '/db/' + self.fName, 'r')
        for i in fl.readlines():
            (key,val) = i.split('|')
            self._theDict[key] = string.strip(val)
        fl.close()

    def save(self):
        """
        save the dictionary to disk
        """
        fl = open(common.work_space.shareDir() + '/db/' + self.fName, 'w')
        for j, k in self._theDict.iteritems():
            fl.write(j + '|' + k + '\n')
        fl.close()

    def setDict(self, key, value):
        """
        assign value to key: key is case sensitive
        """
        self._theDict[key]=value
        
    def dict(self, key):
        """
        assign value to key
        """
        if not self._theDict.has_key(key):
            raise CrabException("TaskDB: key "+key+" not found")
        return self._theDict[key]
