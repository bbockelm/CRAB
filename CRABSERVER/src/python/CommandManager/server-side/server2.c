#include "soapH.h"
#include <pthread.h>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <Python.h>

#define BACKLOG (100) // Max. request backlog 
#define MAX_THR (4) // Size of thread pool 
#define MAX_QUEUE (100) // Max. size of request queue 

SOAP_SOCKET queue[MAX_QUEUE]; // The global request queue of sockets 
int head = 0, tail = 0; // Queue head and tail 

void *process_queue(void*); 
int enqueue(SOAP_SOCKET); 
SOAP_SOCKET dequeue(); 

pthread_mutex_t queue_cs; 
pthread_cond_t queue_cv; 

// global var for backend loading
PyObject *pClass;
PyObject *pInstance;
PyObject* allocInstance(void);
PyObject* loadClass(void);

SOAP_NMAC struct Namespace namespaces[] =
{
        {"SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/", "http://www.w3.org/*/soap-envelope", NULL},
        {"SOAP-ENC", "http://schemas.xmlsoap.org/soap/encoding/", "http://www.w3.org/*/soap-encoding", NULL},
        {"xsi", "http://www.w3.org/2001/XMLSchema-instance", "http://www.w3.org/*/XMLSchema-instance", NULL},
        {"xsd", "http://www.w3.org/2001/XMLSchema", "http://www.w3.org/*/XMLSchema", NULL},
        {"ns1", "http://www.example.org/CRAB-Proxy/", NULL, NULL},
        {NULL, NULL, NULL, NULL}
};

// ----------------------------------------------------------------------------
// WS main methods
// ----------------------------------------------------------------------------

int run_service(int port, char* logFile)  
{ 
	struct soap soap; 
	soap_init(&soap); 
      
	struct soap *soap_thr[MAX_THR]; // each thread needs a runtime environment 
	pthread_t tid[MAX_THR]; 
	SOAP_SOCKET m, s; 
	int i;
	time_t rawtime;
        FILE* wslog;

	// Let's start
        wslog = NULL;
        wslog = fopen(logFile, "a");
        if (wslog == NULL)
	{
                fprintf(stderr, "Error opening logfile\n");
		return 1;        
	}

        fprintf(stdout, "Frontend Service\n");

        // redirect stderr/stdout on wslog, in order have transparent logging for threads
        dup2( fileno(wslog), STDERR_FILENO);
        dup2( fileno(wslog), STDOUT_FILENO);

        fprintf(stdout, "Frontend Service\n");

	// init python interpreter and class definition
        Py_Initialize();
	PyEval_InitThreads();

        pClass = loadClass();
        if ((pClass == NULL) || (PyCallable_Check(pClass) != 1))
        {
                PyErr_Print();
                Py_Finalize();
                fprintf(stdout, "Backend class has no attributes or is not callable\n");
                return 1;
        }
        fprintf(stdout, "Python backend loaded\n");

	// instanciate the backend class
        pInstance = allocInstance();
        if (pInstance == NULL)
        {
                PyErr_Print();
		Py_Finalize();
                fprintf(stdout, "Error while instantiating Backend\n");
                return 1;
        }

        // server loop 
        fprintf(stdout, "Socket binding...");

	m = soap_bind(&soap, NULL, port, BACKLOG); 
	if (!soap_valid_socket(m)) 
	{
                fprintf(stdout, "FAIL\n");
		return 1; //exit(1);
 	}
	fprintf(stdout, "DONE (%d)\n", m);

	pthread_mutex_init(&queue_cs, NULL); 
	pthread_cond_init(&queue_cv, NULL); 
	for (i = 0; i < MAX_THR; i++) 
	{ 
		soap_thr[i] = soap_copy(&soap); 
		fprintf(stdout, "Starting thread %d\n", i); 
		pthread_create(&tid[i], NULL, (void*(*)(void*))process_queue, (void*)soap_thr[i]); 
	} 

	// EVENT HANDLER
	for (;;) 
	{ 
		s = soap_accept(&soap);
		if (!soap_valid_socket(s)) 
		{ 
			if (soap.errnum) 
			{ 
				soap_print_fault(&soap, stdout);
                                fprintf(stdout, "Wrong socket connection\n"); 
				continue; // retry 
			} 
			else
			{ 
				fprintf(stdout, "Server timed out\n"); 
				break; 
			} 
		} 

		time(&rawtime);
		fprintf(stdout, "Thread %d accepts socket %d connection from IP %d.%d.%d.%d TStamp: %s", 
			i, s, (int)(soap.ip >> 24)&0xFF, (int)(soap.ip >> 16)&0xFF, (int)(soap.ip >> 8)&0xFF, (int)soap.ip&0xFF,
			asctime(localtime(&rawtime)) ); 

		while (enqueue(s) == SOAP_EOM) 
			sleep(1); 
	} 

	Py_XDECREF(pInstance);
	Py_XDECREF(pClass);
	Py_Finalize();

	// TERMINATION LOOP
	for (i = 0; i < MAX_THR; i++) 
	{ 
		while (enqueue(SOAP_INVALID_SOCKET) == SOAP_EOM) 
			sleep(1); 
	} 

	for (i = 0; i < MAX_THR; i++) 
	{ 
		fprintf(stdout, "Waiting for thread %d to terminate... ", i); 
		pthread_join(tid[i], NULL); 
		fprintf(stdout, "terminated\n"); 
		soap_done(soap_thr[i]); 
		free(soap_thr[i]); 
	} 

	pthread_mutex_destroy(&queue_cs); 
	pthread_cond_destroy(&queue_cv); 
	soap_done(&soap); 
	fclose(wslog);
	return 0; 
} 

int main(int argc, char **argv)
{
	int port, ret_code;
	char* fileName;

	if (argc < 3)
	{
		printf("Usage: %s port logFilename\n", argv[0]);
                return 0;
	}
	
	port = atoi(argv[1]); 
        fileName = argv[2];
	ret_code = run_service(port, fileName);
	return ret_code; 
}


// AUXILIARY functions for pool management

void *process_queue(void *soap) 
{ 
   struct soap *tsoap = (struct soap*)soap; 
   for (;;) 
   { 
      tsoap->socket = dequeue(); 
      if (!soap_valid_socket(tsoap->socket)) 
         break; 
      soap_serve(tsoap); 
      soap_destroy(tsoap); 
      soap_end(tsoap); 
   } 
   return NULL; 
} 

int enqueue(SOAP_SOCKET sock) 
{ 
   int status = SOAP_OK; 
   int next; 
   pthread_mutex_lock(&queue_cs); 
   next = tail + 1; 
   if (next >= MAX_QUEUE) 
      next = 0; 
   if (next == head) 
      status = SOAP_EOM; 
   else
   { 
      queue[tail] = sock; 
      tail = next; 
   } 
   pthread_cond_signal(&queue_cv); 
   pthread_mutex_unlock(&queue_cs); 
   return status; 
} 

SOAP_SOCKET dequeue() 
{ 
   SOAP_SOCKET sock; 
   pthread_mutex_lock(&queue_cs); 
   while (head == tail)       
       pthread_cond_wait(&queue_cv, &queue_cs); 
   sock = queue[head++]; 
   if (head >= MAX_QUEUE) 
      head = 0; 
   pthread_mutex_unlock(&queue_cs); 
   return sock; 
} 

// ----------------------------------------------------------------------------
// Business logic handlers
// ----------------------------------------------------------------------------

/// Web service operation 'transferTaskAndSubmit' (return error code or SOAP_OK)
int ns1__transferTaskAndSubmit(struct soap *soap, struct ns1__transferTaskType *transferTaskAndSubmitRequest, struct ns1__transferTaskAndSubmitResponse *_param_1) 
{
	char* taskDescriptor;
	char* cmdDescriptor;
	char* UUID;
	char res_str[32];

        PyObject *pResult, *locTemp;
        int res;
	time_t rawtime;

        locTemp = pInstance;

        // Parse input data
	taskDescriptor = transferTaskAndSubmitRequest->taskDescriptor;
	cmdDescriptor = transferTaskAndSubmitRequest->cmdDescriptor;
	UUID = transferTaskAndSubmitRequest->uuid;

	if (locTemp == NULL)
	{
                PyErr_Print();
                fprintf(stderr, "Error while instantiating gway_transferTaskAndSubmit\n");
                res = -1;
	}
	else
	{
		pResult = PyObject_CallMethod(locTemp, "gway_transferTaskAndSubmit", "(sss)", taskDescriptor, cmdDescriptor, UUID);
       		if (pResult == NULL)
	        {
			PyErr_Print();
			fprintf(stderr, "Error while calling gway_transferTaskAndSubmit\n");
			res = -1;
       		} 
		else 
		{
                	res = PyInt_AsLong(pResult);
			Py_XDECREF(pResult);
		}
	}

	// parse back the response code to SOAP
	sprintf(res_str, "%d", res);
	_param_1->transferTaskAndSubmitResponse = (char*)malloc( sizeof(char) * (strlen(res_str)+1) );
        if (_param_1->transferTaskAndSubmitResponse != NULL)
	{
		strcpy( _param_1->transferTaskAndSubmitResponse , res_str );
	}
	else
	{
		fprintf(stderr, "Error while allocating return code location\n");
	}

        time(&rawtime);
	fprintf(stdout, "TransferTaskAndSubmit RPC (%s) TStamp: %s", _param_1->transferTaskAndSubmitResponse, asctime(localtime(&rawtime)) );
	return SOAP_OK;
}

/// Web service operation 'sendCommand' (return error code or SOAP_OK)
int ns1__sendCommand(struct soap *soap, struct ns1__sendCommandType *sendCommandRequest, struct ns1__sendCommandResponse *_param_2) 
{
	char* cmdDescriptor;
	char* UUID;
        PyObject *pResult, *locTemp;
        int res;
        time_t rawtime;
        char res_str[32];

        locTemp = pInstance;

        // summon logic
	cmdDescriptor = sendCommandRequest->cmdDescriptor;
	UUID = sendCommandRequest->uuid;

        if (locTemp == NULL)
        {
                PyErr_Print();
                fprintf(stderr, "Error while instantiating gway_sendCommand\n");
                res = -1;
        }
	else
	{
		pResult = PyObject_CallMethod(locTemp, "gway_sendCommand", "(ss)", cmdDescriptor, UUID);
		if (pResult == NULL)
		{
			PyErr_Print();
			fprintf(stderr, "Error while calling gway_sendCommand\n");
			res = -1;
	        } else {
        	        res = PyInt_AsLong(pResult);
			Py_XDECREF(pResult);
                }
	}

	// prepare response
        sprintf(res_str, "%d", res);
        _param_2->sendCommandResponse = (char*)malloc( sizeof(char) * (strlen(res_str)+1) );
        if (_param_2->sendCommandResponse != NULL)
        {
                strcpy( _param_2->sendCommandResponse , res_str );
        }
        else
        {
                fprintf(stderr, "Error while allocating return code location\n");
        }

	time(&rawtime);
        fprintf(stdout, "SendCommand RPC (%s) TStamp: %s", _param_2->sendCommandResponse, asctime(localtime(&rawtime)) );
        return SOAP_OK;
}

/// Web service operation 'getTaskStatus' (return error code or SOAP_OK)
int ns1__getTaskStatus(struct soap *soap, char *getTaskStatusRequest, struct ns1__getTaskStatusResponse *_param_3)
{
        PyObject *pResult, *locTemp;
	char res[ 8192*16 ];
        time_t rawtime;

        locTemp = pInstance;
	//res = NULL;

	if (locTemp == NULL)
	{
		PyErr_Print();
		fprintf(stderr, "Error while instantiating gway_getTaskStatus\n");
		strcpy(res , "\n");
        }
        else
        {
		pResult = PyObject_CallMethod(locTemp, "gway_getTaskStatus", "(s)", getTaskStatusRequest);
		if (pResult == NULL)
		{
			PyErr_Print();
			fprintf(stderr, "Error while calling gway_getTaskStatus\n");
			strcpy(res , "\n");
		} 
		else 
		{
			long len;
			len =  strlen( PyString_AsString(pResult) );
			// res = (char*)malloc( sizeof(char) * len );
			strncpy(res, PyString_AsString(pResult), len);
                        res[len+1] = '\0';
                        Py_XDECREF(pResult);
                }
	}

	// constuct response
	if (res != NULL)
	{
		_param_3->getTaskStatusResponse = (char*)malloc( sizeof(char) * strlen(res) );
		if ( _param_3->getTaskStatusResponse != NULL)
        	{
                	strncpy( _param_3->getTaskStatusResponse , res, strlen(res) );
	                // free(res);
        	}
	        else
	        {
        	        fprintf(stderr, "Error while allocating return code location\n");
	        }
	}
	else
	{
                fprintf(stderr, "Error while allocating result message location\n");
	}

	time(&rawtime);
	fprintf(stdout, "GetTaskStatus RPC (len=%d) TStamp: %s", strlen(_param_3->getTaskStatusResponse), asctime(localtime(&rawtime)) );
	return SOAP_OK;
}

// ----------------------------------------------------------------------------
// Python wrapping management methods
// ----------------------------------------------------------------------------

PyObject* loadClass(void)
{
	PyObject *pModule, *pc;
	fprintf(stdout, "Loading backend module... ");

	pModule = PyImport_ImportModule("CRAB-CmdMgr-Backend");
	if (pModule == NULL){
		PyErr_Print();
                fprintf(stdout, "FAIL \n");
                fprintf(stderr, "Unable to load backend module. Check if it is in PYTHONPATH\n");
		return NULL;
	}

	pc = PyObject_GetAttrString(pModule, "CRAB_AS_beckend" );
	Py_DECREF(pModule);
	fprintf(stdout, "DONE\n");
	return pc;
}

PyObject* allocInstance(void)
{
	PyObject *pi;
        fprintf(stdout, "Allocating backend instance... ");
	pi = PyObject_CallObject(pClass, Py_BuildValue("()"));
	if (pi == NULL)
	{
		PyErr_Print();
                fprintf(stdout, "FAIL \n");
		fprintf(stderr, "Error while allocating backend instance\n");
		return NULL;
	}
        fprintf(stdout, "DONE\n");
	return pi;        
}


