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
        senderPwd  = notifCfg.get("Notification_SenderPwd")
        
        if senderName == None :
            raise RuntimeError, "missing Notification_SenderName property in " + config + " configuration file"

        if senderPwd == None:
            raise RuntimeError, "missing Notification_SenderPwd property in " + config + " configuration file"
        
        self.senderName = senderName
        self.senderPwd  = senderPwd
        
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
##             server = smtplib.SMTP( self.smtpServer )
##             server.set_debuglevel( self.smtpDbgLvl )
            
##             server.sendmail( self.senderName, toList, message)
##             server.quit()

            server = smtplib.SMTP( self.smtpServer )
            server.set_debuglevel( self.smtpDbgLvl )
            server.ehlo()
            server.starttls()
            server.ehlo() 
            server.login(self.senderName, self.senderPwd);
 
            flag = 1
            for mailt in toList:
                logging.info("Sending to: " + str(mailt) )
                if str(mailt) !=  None or len(str(mailt)) != 0:
                    flag = 0
            if flag == 0:
                server.sendmail(self.senderName, toList, message)
            else: 
                logging.error("No e-mail address specified...")
            server.quit()
            
        except SMTPException, ex:
            errmsg = "SMTP ERROR! " + str(ex)
            raise RuntimeError, errmsg
