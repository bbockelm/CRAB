#
# CRAB-Proxy makefile
# $Id: Makefile,v 1.4 2008/07/08 14:26:59 farinafa Exp $
# Fabio Farina 2007/12/10 

O_NAME = CRAB-CmdMgr-Frontend

#PYHOME = /home/crab/sw_area/slc4_ia32_gcc345/external/python/2.4.3
PYHOME = $(PYTHON_ROOT)

CFLAGS=-Wall -g -DWITH_OPENSSL -pthread -fPIC #-DSOAP_DEBUG #-DSOAP_MEM_DEBUG #-DGLITE_FUNCS #-DSOAP_DEBUG
IFLAGS=-I/opt/glite/include -I/usr/kerberos/include -I/usr/include/libxml2 -I$(PYHOME)/include/python2.4 

# already there for ssl or WS-Security integration
LIBS=-lssl -lcrypto -lxml2 -lpthread -lpython2.4
SERVER_LIBS =-L/opt/glite/lib -L/opt/globus/lib -L$(PYHOME)/lib

all: server_lib install_lib 

server: server2.c
	gcc -o $(O_NAME) server2.c stdsoap2.c soapC.c soapServer.c\
		$(CFLAGS) $(IFLAGS) -export-dynamic \
		$(LIBS) $(SERVER_LIBS)

server_lib: server2.c
	gcc -c -o FrontendLoader.o FrontendLoader.c $(CFLAGS) $(IFLAGS)
	gcc -shared -o _FrontendLoader.so FrontendLoader.o\
		stdsoap2.c soapC.c soapServer.c server2.c\
		$(CFLAGS) $(IFLAGS) -export-dynamic \
		$(LIBS) $(SERVER_LIBS) 

install:
	echo "Installing server-side files"
	mkdir -p ../bin/server
	cp $(O_NAME) ../bin/server
	cp CRAB-CmdMgr-Backend.py ../bin/server
	cp CmdMgrComponent.py ../bin/server
	cp JabberThread.py ../bin/server
	cp Startup.py ../bin/server
	cp __init__.py ../bin/server

install_lib:
	echo "Installing server-side libs"
	mkdir -p ../bin/server
	cp CRAB-CmdMgr-Backend.py ../bin/server
	cp CmdMgrComponent.py ../bin/server
	cp JabberThread.py ../bin/server
	cp Startup.py ../bin/server
	cp __init__.py ../bin/server
	cp _FrontendLoader.so ../bin/server
	cp FrontendLoader.py ../bin/server

clean:
	rm -f *.o $(O_NAME) *.so *.pyc

rebuild:
	clean all

