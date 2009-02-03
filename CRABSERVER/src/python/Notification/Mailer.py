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
    def SendMail(self, toList, subject, message):
        if len(toList) < 1:
            logging.error("1: No e-mail address specified")
            return
        elif len(toList[0]) < 1:
            logging.error("2: No e-mail address specified")
            return 
        if str(self.smtpServer).strip() != "localhost":
            logging.info("Sending through '%s'" %str(self.smtpServer))
            try:
                emailaddr = ",".join( toList )
                server = smtplib.SMTP( self.smtpServer )
                server.set_debuglevel( self.smtpDbgLvl )
                server.ehlo()
                server.starttls()
                server.ehlo() 
                server.login(self.senderName, self.senderPwd);
 
                flag = 1
                for mailt in emailaddr:
                    if mailt ==  None or len(str(mailt)) < 1:
                        flag = -1
                if flag == 1:
                    complMsg = "Subject:\""+str(subject)+"\n\n" + message
                    server.sendmail(self.senderName, toList, complMsg)
                else: 
                    logging.error("Not sending: NO e-mail address specified...")
                server.quit()
            except SMTPException, ex:
                errmsg = "SMTP ERROR! " + str(ex)
                raise RuntimeError, errmsg
            except Exception, ex:
                errmsg = "Generic ERROR!" + str(ex)
                raise Exception, errmsg

        else:
            import time, os
            infoFile = "/tmp/crabNotifInfoFile." + str(time.time())

            try:
                os.remove(infoFile)
            except OSError:
                pass

            FILE = open(infoFile,"w")
            FILE.write(message)
            FILE.close()

            mainEmail = toList.pop(0)
            CCRecipients = ",".join( toList )

            if len(toList) >=2:
                cmd = "mail -s \"%s\" "%str(subject)
                cmd += mainEmail + " -c " + CCRecipients + " < " + infoFile
            else:
                cmd = "mail -s \"%s\" "%str(subject)
                cmd += mainEmail + " < " + infoFile

            msg = "Notification.Consumer.Notify: Sending mail to [" + str(toList) + "]"
            logging.info( msg )
            logging.info( cmd )
                
            retCode = os.system( cmd )

            if(retCode != 0):
                errmsg = "ERROR! Command ["+cmd+"] FAILED!"
                logging.error(errmsg)

            try:
                os.remove(infoFile)
            except OSError:
                pass

