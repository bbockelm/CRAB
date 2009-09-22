#!/usr/bin/env python
"""
_LoadTaskNameByJgId_

MySQL implementation of CrabJobCreator.LoadTaskNameByJgId 
"""
__all__ = []
__revision__ = "$Id: LoadTaskNameByJgId,v 0 2009/09/22 00:46:10 riahi Exp $"
__version__ = "$Revision: 0 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadTaskNameByJgId(DBFormatter):
    """
    Load Task name wmbs DB by JobGroup Id
    """
    sql = """SELECT Task FROM wmbs_workflow 
              WHERE id IN (SELECT workflow FROM wmbs_subscription WHERE \
              id = (SELECT subscription FROM wmbs_jobgroup WHERE id = :id)) 
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


