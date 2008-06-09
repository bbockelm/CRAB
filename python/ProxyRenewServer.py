from Actor import *
from SubmitterServer import SubmitterServer
from crab_util import *
import common

import os, errno, time, sys, re
import commands

class ProxyRenewServer(SubmitterServer):

    def __init__(self, cfg_params):
        SubmitterServer.__init__(self, cfg_params, None, "all")

    def run(self):
        common.logger.debug(5, "ProxyRenewServer::run() called")
        self.moveProxy(self.dontMoveProxy)

