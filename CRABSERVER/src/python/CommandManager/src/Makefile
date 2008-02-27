#
# CRAB-Proxy makefile
# $Id: Makefile,v 1.0 2007/12/10 11:40:10 farinafa Exp $
# Fabio Farina 2007/12/10 

O_NAME = CRAB-AS-CmdMgr-Frontend
PYHOME = /home/crab/sw_area/slc4_ia32_gcc345/external/python/2.4.3

CFLAGS=-Wall -g -DWITH_OPENSSL -pthread -fPIC #-DSOAP_DEBUG #-DSOAP_MEM_DEBUG #-DGLITE_FUNCS #-DSOAP_DEBUG
IFLAGS=-I/opt/glite/include -I/usr/kerberos/include -I/usr/include/libxml2 -I$(PYHOME)/include/python2.4 

# already there for ssl or WS-Security integration
LIBS=-lssl -lcrypto -lxml2 -lpthread -lpython2.4
SERVER_LIBS =-L/opt/glite/lib -L/opt/globus/lib -L$(PYHOME)/lib

all: server client install 

client: CRAB_Proxy_API.h
#	swig -c++ -python -o CRAB_Server_API.cpp CRAB_Server_API.i
	g++ -c -o CRAB_Server_API.o CRAB_Server_API.cpp $(CFLAGS) $(IFLAGS)
	g++ $(CFLAGS) -shared -o _CRAB_Server_API.so CRAB_Server_API.o \
		CRAB_ProxySOAP.nsmap.cpp soapCRAB_ProxySOAPProxy.cpp CRAB_Proxy_API.cpp \
		soapC.cpp stdsoap2.cpp $(LIBS) $(SERVER_LIBS) 

server: CRAB-Proxy-server.cpp soapCRAB_ProxySOAPService.cpp
	g++ -c -o soapCRAB_ProxySOAPService.o soapCRAB_ProxySOAPService.cpp $(CFLAGS) $(IFLAGS) -export-dynamic
	g++ -o $(O_NAME) soapCRAB_ProxySOAPService.o CRAB-Proxy-server.cpp soapC.cpp stdsoap2.cpp \
		$(CFLAGS) $(IFLAGS) -export-dynamic \
		$(LIBS) $(SERVER_LIBS)

testclient: testClient.cpp 
	g++ -o testClient testClient.cpp soapCRAB_ProxySOAPProxy.cpp soapC.cpp stdsoap2.cpp \
		$(CFLAGS) $(IFLAGS) $(LIBS) $(SERVER_LIBS)
install:
	echo "Installing server-side files" 
	cp $(O_NAME) ../.
	cp CRAB-CmdMgr-Backend.py ../.
	cp CmdMgrComponent.py ../.
	cp JabberThread.py ../.
	cp Startup.py ../.
	cp __init__.py ../.
	echo "Installing client-side files"
	mkdir -p ../client
	cp CRAB_Server_API.py ../client/.
	cp _CRAB_Server_API.so ../client/.
	cp ServerCommunicator.py ../client/. 

clean:
	rm -f *.o  $(O_NAME) *.so
#	rm -f CRAB_Server_API.cpp CRAB_Server_API.py CRAB_Server_API.pyc 

rebuild:
	clean all

