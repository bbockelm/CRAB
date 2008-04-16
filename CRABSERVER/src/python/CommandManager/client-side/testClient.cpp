#include <iostream>
#include "CRAB_Proxy_API.h"

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
	CRAB_Server_Session* cs = new CRAB_Server_Session("localhost", 20079);	 
        cout << "Session Ready" << endl;

	int res = cs->transferTaskAndSubmit("string taskXML", "string cmdXML", "string taskUniqName");
	cout << "cs.transferTaskAndSubmit " << res << endl;

	res = cs->sendCommand("string cmdXML", "string taskUniqName");
        cout << "cs.sendCommand " << res << endl;

        cout << "cs.getTaskStatus " << cs->getTaskStatus("string taskUniqName") << endl;

	delete cs;
	return 0; 
}
 
