#!/usr/bin/env python
"""
_ListToSplit_

MySQL implementation of CrabJobCreator.ListToSplit
"""

__all__ = []
__revision__ = "$Id: ListToSplit.py,v 0 2009/09/22 00:46:10 riahi Exp $"
__version__ = "$Revision: 0 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListToSplit(DBFormatter):
    """
    List of all subscription which can be splitted
    """  
    sql = "SELECT id FROM wmbs_subscription WHERE wmbs_subscription.fileset \
          IN (SELECT id FROM wmbs_fileset WHERE open = 0) AND wmbs_subscription.id\
          NOT IN (SELECT subscription FROM wmbs_jobgroup)"
    
    def format(self, result):
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            subIDs.append(row[0])

        return subIDs
        
    def execute(self, conn = None, transaction = False):
        """
        Execute query
        """
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.format(result)
