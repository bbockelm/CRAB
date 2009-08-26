#
# CRAB-Proxy makefile
# $Id: Makefile,v 1.1 2008/04/16 17:22:11 farinafa Exp $
# Fabio Farina 2007/12/10 

PYHOME = /home/crab/sw_area/slc4_ia32_gcc345/external/python/2.4.3

CFLAGS=-Wall -g -DWITH_OPENSSL -pthread -fPIC #-DSOAP_DEBUG #-DSOAP_MEM_DEBUG #-DGLITE_FUNCS #-DSOAP_DEBUG
IFLAGS=-I/opt/glite/include -I/usr/kerberos/include -I/usr/include/libxml2 -I$(PYHOME)/include/python2.4 

# already there for ssl or WS-Security integration
LIBS=-lssl -lcrypto -lxml2 -lpthread -lpython2.4
SERVER_LIBS =-L/opt/glite/lib -L/opt/globus/lib -L$(PYHOME)/lib

all: client install 

client: CRAB_Proxy_API.h
	g++ -c -o CRAB_Server_API.o CRAB_Server_API.cpp $(CFLAGS) $(IFLAGS)
	g++ $(CFLAGS) -shared -o _CRAB_Server_API.so CRAB_Server_API.o \
		CRAB_ProxySOAP.nsmap.cpp soapCRAB_ProxySOAPProxy.cpp CRAB_Proxy_API.cpp \
		soapC.cpp stdsoap2.cpp $(LIBS) $(SERVER_LIBS) 

install:
	echo "Installing client-side files"
	mkdir -p ../bin/client
	cp CRAB_Server_API.py ../bin/client/.
	cp _CRAB_Server_API.so ../bin/client/.

clean:
	rm -f *.o  $(O_NAME) *.so

rebuild:
	clean all

testclient: testClient.cpp
	g++ -c -o CRAB_Server_API.o CRAB_Server_API.cpp $(CFLAGS) $(IFLAGS)
	g++ -o testClient testClient.cpp CRAB_Server_API.o soapCRAB_ProxySOAPProxy.cpp \
		CRAB_Proxy_API.cpp soapC.cpp stdsoap2.cpp $(CFLAGS) $(IFLAGS) $(LIBS) $(SERVER_LIBS)

