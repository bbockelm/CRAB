#!/usr/bin/env python
"""
_ListToSplit_

MySQL implementation of CrabJobCreator.ListToSplit
"""

__all__ = []
__revision__ = "$Id: ListToSplit.py,v 1.1 2009/09/21 23:59:38 riahi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListToSplit(DBFormatter):
    """
    List of all subscription which can be splitted
    """  
    sql = """SELECT id FROM wmbs_subscription
               INNER JOIN (SELECT fileset, COUNT(file) AS total_files
                           FROM wmbs_fileset_files GROUP BY fileset) wmbs_fileset_total_files
                 ON wmbs_subscription.fileset = wmbs_fileset_total_files.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS complete_files
                           FROM wmbs_sub_files_complete GROUP BY subscription) wmbs_files_complete
                 ON wmbs_subscription.id = wmbs_files_complete.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS failed_files
                           FROM wmbs_sub_files_failed GROUP BY subscription) wmbs_files_failed
                 ON wmbs_subscription.id = wmbs_files_failed.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS acquired_files
                           FROM wmbs_sub_files_acquired GROUP BY subscription) wmbs_files_acquired
                 ON wmbs_subscription.id = wmbs_files_acquired.subscription 
             WHERE total_files != COALESCE(complete_files, 0) + COALESCE(failed_files, 0) +
                                  COALESCE(acquired_files, 0) AND wmbs_subscription.id >= :minsub
             AND workflow IN (SELECT workflow FROM wm_managed_workflow )"""

    def format(self, result):
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            subIDs.append(row[0])

        return subIDs

    def execute(self, minSub = 0, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, binds = {"minsub": minSub},
                                      conn = conn, transaction = transaction)
        return self.format(result)