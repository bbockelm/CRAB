#include "CRAB_Proxy_API.h"

using namespace std;

CRAB_Server_Session::CRAB_Server_Session(string URL, int port)
	{
		stringstream ss;
		ss << "http://" << URL << ":" << port << "/CRAB-Proxy";

		px = new CRAB_ProxySOAPProxy();
		endpoint = string(ss.str().c_str());
		px->soap_endpoint =  const_cast<char*>( endpoint.c_str() );
	}

CRAB_Server_Session::~CRAB_Server_Session()
	{
		delete px;
	}

/// Exposed Operations
int CRAB_Server_Session::transferTaskAndSubmit(string taskXML, string cmdXML, string taskUniqName)
	{
		ns1__transferTaskType* reqT = new ns1__transferTaskType();
		struct ns1__transferTaskAndSubmitResponse resT;

	        reqT->taskDescriptor = const_cast<char*>(taskXML.c_str());
        	reqT->cmdDescriptor = const_cast<char*>(cmdXML.c_str());
	        reqT->uuid = const_cast<char*>(taskUniqName.c_str());
	

		if (px->transferTaskAndSubmit(reqT, resT) == SOAP_OK)
		{ 
			stringstream ss;
			ss << resT.transferTaskAndSubmitResponse;
			int ret;
			ss >> ret;
			return ret; 
		}
 
		return 12; // SOAP communication error
	}

int CRAB_Server_Session::sendCommand(string cmdXML, string taskUniqName)
	{
		ns1__sendCommandType* reqC = new ns1__sendCommandType(); 
		struct ns1__sendCommandResponse resC;

                reqC->cmdDescriptor = const_cast<char*>(cmdXML.c_str());
                reqC->uuid = const_cast<char*>(taskUniqName.c_str());

		if (px->sendCommand(reqC, resC) == SOAP_OK)
		{
			stringstream ss;
			ss << resC.sendCommandResponse;
			int ret;
			ss >> ret;
			return ret;
		}

		return 22; // SOAP communication error
	}

string CRAB_Server_Session::getTaskStatus(string taskUniqName)
	{
		struct ns1__getTaskStatusResponse resS;

		if (px->getTaskStatus(const_cast<char*>(taskUniqName.c_str()), resS) == SOAP_OK)
		{
			stringstream ss;
			ss << resS.getTaskStatusResponse;
			return ss.str();
		}

		return string("Error: problem during SOAP communication");
	}

