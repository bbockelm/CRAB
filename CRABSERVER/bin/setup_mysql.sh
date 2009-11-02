#!/bin/sh

export DBNAME=wmbs
export DBUSER=root
export DBPASS=
export DIALECT=MySQL
export DBHOST=localhost
export DBSOCK=$TESTDIR/mysqldata/mysql.sock
export DBMASTERUSER=root
export DBMASTERPASS=
export PROXYDB=CrabServerDB
export PROXYDATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${PROXYDB}
export PROXYCREATE="GRANT ALL PRIVILEGES ON ${PROXYDB}.* TO '${DBUSER}'@'$DBHOST' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"

#Creating MySQL database access string here
export DATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${DBNAME}
export SQLCREATE="GRANT ALL PRIVILEGES ON ${DBNAME}.* TO '${DBUSER}'@'$DBHOST' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"
export GRANTSUPER="GRANT SUPER on *.* to '${DBUSER}'@'$DBHOST';"

