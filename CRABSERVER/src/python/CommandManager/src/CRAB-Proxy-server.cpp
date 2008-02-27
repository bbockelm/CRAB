#include "soapH.h"
#include "soapCRAB_ProxySOAPService.h"

#include <iostream>
#include <csignal> 
#include <cstdlib>

#include <Python.h>

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

void cleanUp(int dummy) 
{
	cout << "Termination signal catched. Halting the server..." << endl;
	exit(-1);
}

void printHelp(char* argv[])
{
	cout << "Usage: "<< argv[0] << " port" << endl;
}

int main(int argc, char* argv[])
{
	// signal handlers
	signal( SIGTERM, cleanUp );  
	signal( SIGINT, cleanUp );  
	signal( SIGQUIT, cleanUp );  
	signal( SIGHUP, cleanUp ); 
 
	if (argc < 2)
	{
		printHelp(argv);
		return -1;
	}
	
	cout << "CRAB Server WS Frontend Starting..." << endl;
	Py_SetProgramName(argv[0]);
        //PySys_SetPath(Py_GetPath());
 
	SOAP_CMAC CRAB_ProxySOAPService* cps = new  SOAP_CMAC CRAB_ProxySOAPService();

	int port = -1;
	port = atoi(argv[1]);

	if (port==0 )
	{
		cout << "Unable to parse the port number" << endl;
		printHelp(argv);
		return -1;
	}

        cps->run(port);
	return 0;
}

