#include "soapH.h"
#include <iostream>
#include <csignal>
#include "soapCRAB_ProxySOAPProxy.h"

using namespace std;

SOAP_NMAC struct Namespace namespaces[] =
{
        {"SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/", "http://www.w3.org/*/soap-envelope", NULL},
        {"SOAP-ENC", "http://schemas.xmlsoap.org/soap/encoding/", "http://www.w3.org/*/soap-encoding", NULL},
        {"xsi", "http://www.w3.org/2001/XMLSchema-instance", "http://www.w3.org/*/XMLSchema-instance", NULL},
        {"xsd", "http://www.w3.org/2001/XMLSchema", "http://www.w3.org/*/XMLSchema", NULL},
        {"ns1", "http://www.example.org/CRAB-Proxy/", NULL, NULL},
        {NULL, NULL, NULL, NULL}
};

int main() 
{ 
	SOAP_CMAC CRAB_ProxySOAPProxy px;
	px.soap_endpoint = "http://localhost:2180/CRAB-Proxy";

	ns1__transferTaskType* reqT = new ns1__transferTaskType();
	struct ns1__transferTaskAndSubmitResponse resT;

        reqT->taskDescriptor = "reqT XML for Task\n";
        reqT->cmdDescriptor = "cmd XML\n";
        reqT->uuid = "UUID code\n";
	

	if (px.transferTaskAndSubmit(reqT, resT) == SOAP_OK)
	{ 
		cout << "OK " << endl; 
		cout << "result code: " << resT.transferTaskAndSubmitResponse << endl; 
	} else
		cout << "Err " << endl;
	return 0; 
}
 
