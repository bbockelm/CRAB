#!/usr/bin/env python

import smtplib

from smtplib import SMTPException

import logging
from ProdAgentCore.Configuration import ProdAgentConfiguration

"""
_Mailer_

"""
class Mailer:

    ##--------------------------------------------------------------------------
    def __init__(self, config):
        
        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)
        notifCfg = cfgObject.getConfig("Notification")
        
        senderName = notifCfg.get("Notification_SenderName")

        if senderName == None :
            raise RuntimeError, "missing Notification_SenderName property in " + config + " configuration file"
        
        self.senderName = senderName

        
        self.smtpServer = notifCfg.get("Notification_SMTPServer")

        if self.smtpServer == None :
            raise RuntimeError, "missing Notification_SMTPServer property in " + config + " configuration file"

        self.smtpDbgLvl = notifCfg.get("Notification_SMTPServerDBGLVL")
        if self.smtpDbgLvl == None:
            self.smtpDbgLvl = 0
        

        msg = "Notification.Mailer: mail Sender is [" + self.senderName + "]"
        logging.info( msg )
        msg = "Notification.Mailer: SMTP server is [" + self.smtpServer + "]"
        logging.info( msg )
        msg = "Notification.Mailer: SMTP server debug level [" + self.smtpDbgLvl + "]"
        logging.info( msg )
        
    ##--------------------------------------------------------------------------
    def SendMail(self, toList, message):
        
        try:
            server = smtplib.SMTP( self.smtpServer )
            server.set_debuglevel( self.smtpDbgLvl )
            
            #msg = "Notification.Mailer: Going to call server.SendMail(" + self.senderName + ", "+",".join(toList)+")"
            #logging.info( msg )
            
            
            server.sendmail( self.senderName, toList, message)
            server.quit()
            
        except SMTPException, ex:
            errmsg = "SMTP ERROR! " + str(ex)
            raise RuntimeError, errmsg
