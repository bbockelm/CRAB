
#ifndef CRAB_Proxy_API_H
#define CRAB_Proxy_API_H

#include "soapH.h"
#include "soapCRAB_ProxySOAPProxy.h"

#include <string>
#include <sstream>

using namespace std;

class CRAB_Server_Session
{ 

public:
	CRAB_Server_Session(string URL, int port);
	virtual ~CRAB_Server_Session();

	/// Exposed Operations
	virtual int transferTaskAndSubmit(string taskXML, string cmdXML, string taskUniqName);
	virtual int sendCommand(string cmdXML, string taskUniqName);
	virtual string getTaskStatus(string statusType, string taskUniqName);

private:
        string endpoint;
        CRAB_ProxySOAPProxy* px;		

};
#endif
