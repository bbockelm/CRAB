/* soapCRAB_ProxySOAPProxy.cpp
   Generated by gSOAP 2.7.9l from CRAB-Proxy.h
   Copyright(C) 2000-2007, Robert van Engelen, Genivia Inc. All Rights Reserved.
   This part of the software is released under one of the following licenses:
   GPL, the gSOAP public license, or Genivia's license for commercial use.
*/

#include "soapCRAB_ProxySOAPProxy.h"

CRAB_ProxySOAPProxy::CRAB_ProxySOAPProxy()
{	CRAB_ProxySOAPProxy_init(SOAP_IO_DEFAULT, SOAP_IO_DEFAULT);
}

CRAB_ProxySOAPProxy::CRAB_ProxySOAPProxy(soap_mode iomode)
{	CRAB_ProxySOAPProxy_init(iomode, iomode);
}

CRAB_ProxySOAPProxy::CRAB_ProxySOAPProxy(soap_mode imode, soap_mode omode)
{	CRAB_ProxySOAPProxy_init(imode, omode);
}

void CRAB_ProxySOAPProxy::CRAB_ProxySOAPProxy_init(soap_mode imode, soap_mode omode)
{	soap_imode(this, imode);
	soap_omode(this, omode);
	soap_endpoint = NULL;
	static const struct Namespace namespaces[] =
{
	{"SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/", "http://www.w3.org/*/soap-envelope", NULL},
	{"SOAP-ENC", "http://schemas.xmlsoap.org/soap/encoding/", "http://www.w3.org/*/soap-encoding", NULL},
	{"xsi", "http://www.w3.org/2001/XMLSchema-instance", "http://www.w3.org/*/XMLSchema-instance", NULL},
	{"xsd", "http://www.w3.org/2001/XMLSchema", "http://www.w3.org/*/XMLSchema", NULL},
	{"ns1", "http://www.example.org/CRAB-Proxy/", NULL, NULL},
	{NULL, NULL, NULL, NULL}
};
	if (!this->namespaces)
		this->namespaces = namespaces;
}

CRAB_ProxySOAPProxy::~CRAB_ProxySOAPProxy()
{ }

void CRAB_ProxySOAPProxy::soap_noheader()
{	header = NULL;
}

const SOAP_ENV__Fault *CRAB_ProxySOAPProxy::soap_fault()
{	return this->fault;
}

const char *CRAB_ProxySOAPProxy::soap_fault_string()
{	return *soap_faultstring(this);
}

const char *CRAB_ProxySOAPProxy::soap_fault_detail()
{	return *soap_faultdetail(this);
}

int CRAB_ProxySOAPProxy::transferTaskAndSubmit(ns1__transferTaskType *transferTaskAndSubmitRequest, struct ns1__transferTaskAndSubmitResponse &_param_1)
{	struct soap *soap = this;
	struct ns1__transferTaskAndSubmit soap_tmp_ns1__transferTaskAndSubmit;
	const char *soap_action = NULL;
	if (!soap_endpoint)
		soap_endpoint = "http://www.example.org/";
	soap_action = "http://www.example.org/CRAB-Proxy/transferTaskAndSubmit";
	soap->encodingStyle = NULL;
	soap_tmp_ns1__transferTaskAndSubmit.transferTaskAndSubmitRequest = transferTaskAndSubmitRequest;
	soap_begin(soap);
	soap_serializeheader(soap);
	soap_serialize_ns1__transferTaskAndSubmit(soap, &soap_tmp_ns1__transferTaskAndSubmit);
	if (soap_begin_count(soap))
		return soap->error;
	if (soap->mode & SOAP_IO_LENGTH)
	{	if (soap_envelope_begin_out(soap)
		 || soap_putheader(soap)
		 || soap_body_begin_out(soap)
		 || soap_put_ns1__transferTaskAndSubmit(soap, &soap_tmp_ns1__transferTaskAndSubmit, "ns1:transferTaskAndSubmit", "")
		 || soap_body_end_out(soap)
		 || soap_envelope_end_out(soap))
			 return soap->error;
	}
	if (soap_end_count(soap))
		return soap->error;
	if (soap_connect(soap, soap_endpoint, soap_action)
	 || soap_envelope_begin_out(soap)
	 || soap_putheader(soap)
	 || soap_body_begin_out(soap)
	 || soap_put_ns1__transferTaskAndSubmit(soap, &soap_tmp_ns1__transferTaskAndSubmit, "ns1:transferTaskAndSubmit", "")
	 || soap_body_end_out(soap)
	 || soap_envelope_end_out(soap)
	 || soap_end_send(soap))
		return soap_closesock(soap);
	soap_default_ns1__transferTaskAndSubmitResponse(soap, &_param_1);
	if (soap_begin_recv(soap)
	 || soap_envelope_begin_in(soap)
	 || soap_recv_header(soap)
	 || soap_body_begin_in(soap))
		return soap_closesock(soap);
	soap_get_ns1__transferTaskAndSubmitResponse(soap, &_param_1, "ns1:transferTaskAndSubmitResponse", "");
	if (soap->error)
	{	if (soap->error == SOAP_TAG_MISMATCH && soap->level == 2)
			return soap_recv_fault(soap);
		return soap_closesock(soap);
	}
	if (soap_body_end_in(soap)
	 || soap_envelope_end_in(soap)
	 || soap_end_recv(soap))
		return soap_closesock(soap);
	return soap_closesock(soap);
}

int CRAB_ProxySOAPProxy::sendCommand(ns1__sendCommandType *sendCommandRequest, struct ns1__sendCommandResponse &_param_2)
{	struct soap *soap = this;
	struct ns1__sendCommand soap_tmp_ns1__sendCommand;
	const char *soap_action = NULL;
	if (!soap_endpoint)
		soap_endpoint = "http://www.example.org/";
	soap_action = "http://www.example.org/CRAB-Proxy/sendCommand";
	soap->encodingStyle = NULL;
	soap_tmp_ns1__sendCommand.sendCommandRequest = sendCommandRequest;
	soap_begin(soap);
	soap_serializeheader(soap);
	soap_serialize_ns1__sendCommand(soap, &soap_tmp_ns1__sendCommand);
	if (soap_begin_count(soap))
		return soap->error;
	if (soap->mode & SOAP_IO_LENGTH)
	{	if (soap_envelope_begin_out(soap)
		 || soap_putheader(soap)
		 || soap_body_begin_out(soap)
		 || soap_put_ns1__sendCommand(soap, &soap_tmp_ns1__sendCommand, "ns1:sendCommand", "")
		 || soap_body_end_out(soap)
		 || soap_envelope_end_out(soap))
			 return soap->error;
	}
	if (soap_end_count(soap))
		return soap->error;
	if (soap_connect(soap, soap_endpoint, soap_action)
	 || soap_envelope_begin_out(soap)
	 || soap_putheader(soap)
	 || soap_body_begin_out(soap)
	 || soap_put_ns1__sendCommand(soap, &soap_tmp_ns1__sendCommand, "ns1:sendCommand", "")
	 || soap_body_end_out(soap)
	 || soap_envelope_end_out(soap)
	 || soap_end_send(soap))
		return soap_closesock(soap);
	soap_default_ns1__sendCommandResponse(soap, &_param_2);
	if (soap_begin_recv(soap)
	 || soap_envelope_begin_in(soap)
	 || soap_recv_header(soap)
	 || soap_body_begin_in(soap))
		return soap_closesock(soap);
	soap_get_ns1__sendCommandResponse(soap, &_param_2, "ns1:sendCommandResponse", "");
	if (soap->error)
	{	if (soap->error == SOAP_TAG_MISMATCH && soap->level == 2)
			return soap_recv_fault(soap);
		return soap_closesock(soap);
	}
	if (soap_body_end_in(soap)
	 || soap_envelope_end_in(soap)
	 || soap_end_recv(soap))
		return soap_closesock(soap);
	return soap_closesock(soap);
}

int CRAB_ProxySOAPProxy::getTaskStatus(ns1__getTaskStatusType *getTaskStatusRequest, struct ns1__getTaskStatusResponse &_param_3)
{	struct soap *soap = this;
	struct ns1__getTaskStatus soap_tmp_ns1__getTaskStatus;
	const char *soap_action = NULL;
	if (!soap_endpoint)
		soap_endpoint = "http://www.example.org/";
	soap_action = "http://www.example.org/CRAB-Proxy/getTaskStatus";
	soap->encodingStyle = NULL;
	soap_tmp_ns1__getTaskStatus.getTaskStatusRequest = getTaskStatusRequest;
	soap_begin(soap);
	soap_serializeheader(soap);
	soap_serialize_ns1__getTaskStatus(soap, &soap_tmp_ns1__getTaskStatus);
	if (soap_begin_count(soap))
		return soap->error;
	if (soap->mode & SOAP_IO_LENGTH)
	{	if (soap_envelope_begin_out(soap)
		 || soap_putheader(soap)
		 || soap_body_begin_out(soap)
		 || soap_put_ns1__getTaskStatus(soap, &soap_tmp_ns1__getTaskStatus, "ns1:getTaskStatus", "")
		 || soap_body_end_out(soap)
		 || soap_envelope_end_out(soap))
			 return soap->error;
	}
	if (soap_end_count(soap))
		return soap->error;
	if (soap_connect(soap, soap_endpoint, soap_action)
	 || soap_envelope_begin_out(soap)
	 || soap_putheader(soap)
	 || soap_body_begin_out(soap)
	 || soap_put_ns1__getTaskStatus(soap, &soap_tmp_ns1__getTaskStatus, "ns1:getTaskStatus", "")
	 || soap_body_end_out(soap)
	 || soap_envelope_end_out(soap)
	 || soap_end_send(soap))
		return soap_closesock(soap);
	soap_default_ns1__getTaskStatusResponse(soap, &_param_3);
	if (soap_begin_recv(soap)
	 || soap_envelope_begin_in(soap)
	 || soap_recv_header(soap)
	 || soap_body_begin_in(soap))
		return soap_closesock(soap);
	soap_get_ns1__getTaskStatusResponse(soap, &_param_3, "ns1:getTaskStatusResponse", "");
	if (soap->error)
	{	if (soap->error == SOAP_TAG_MISMATCH && soap->level == 2)
			return soap_recv_fault(soap);
		return soap_closesock(soap);
	}
	if (soap_body_end_in(soap)
	 || soap_envelope_end_in(soap)
	 || soap_end_recv(soap))
		return soap_closesock(soap);
	return soap_closesock(soap);
}
/* End of client proxy code */
