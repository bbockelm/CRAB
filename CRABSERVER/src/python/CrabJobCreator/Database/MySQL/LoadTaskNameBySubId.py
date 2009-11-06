#!/usr/bin/env python
"""
_LoadTaskNameByJgId_

MySQL implementation of CrabJobCreator.LoadTaskNameBySubId 
"""
__all__ = []
__revision__ = "$Id: LoadTaskNameByJgId.py,v 1.0 2009/11/06 12:01:20 riahi Exp $"
__version__ = "$Revision: 1.0 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadTaskNameBySubId(DBFormatter):
    """
    Load Task name wmbs DB by subscription Id
    """
    sql = """SELECT Task FROM wmbs_workflow 
              WHERE id = (SELECT workflow FROM wmbs_subscription WHERE \
              id = :id) 
              """
    
    def format(self, result):
        results = DBFormatter.format(self, result)

        tasks = []
        for row in results:
            tasks.append(row[0])

        return tasks
        
    def execute(self, id = None, conn = None, transaction = False):
        """
        Execute query
        """
        result = self.dbi.processData(self.sql, self.getBinds(id = id), \
                                       conn = conn, transaction = transaction)
        return self.format(result)


