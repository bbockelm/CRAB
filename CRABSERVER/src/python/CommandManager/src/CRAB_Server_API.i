%module CRAB_Server_API

%include "std_string.i"

%{
#include "CRAB_Proxy_API.h"
#include "soapH.h"
#include "soapCRAB_ProxySOAPProxy.h"
%}

%include "CRAB_Proxy_API.h"

