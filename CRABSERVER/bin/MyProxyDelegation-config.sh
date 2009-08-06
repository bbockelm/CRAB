#!/bin/sh

if [ `whoami` != 'root' ]; then
    echo "You must to be root in order to configure MyProxyDelegation for CRAB-Server."
    echo "Exiting. "
    exit 1;
fi

if [ $# -eq 0 ]; then
    echo " Parameter(s) needed. For usage see :"
    echo " $0 -help"
    exit 1
fi

while [ $# -gt 0 ]; do
    case $1 in
        install )
            command=install
            shift;;
        uninstall )
            command=uninstall
            shift;;
        -user )
            [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }           
            crabuser="$2"
            shift; shift ;;
        -homeuser )
            [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }
            crabhome="$2"
            shift; shift ;;
        -help )
            cat << \EOF_HELP

MyProxyDelegation-config.sh 
A script to install/uninstall and configure the MyProxy-based delegation.

* Installation Syntax:

MyProxyDelegation-config.sh install -user <crabuser> -homeuser <crabuser Home dir>

-user <crabuser>: user name running the CRAB-Server 

-homeuser <crabuser Home dir>: home directory for the user running the CRAB-Server

* Uninstall Syntax:

MyProxyDelegation-config.sh uninstall

EOF_HELP
            exit 1
            ;;
        * )
            echo "$0: argument $1 not supported"; exit 1;;
    esac
done


##################
### Installation & Configuration
##################
install(){

if ! [ $crabuser ]; then
 echo ""
 echo " -user option is missing. For usage see :"
 echo " $0 -help"
 exit 1
fi

if ! [ $crabhome ]; then
 echo ""
 echo " -homeuser option is missing. For usage see :"
 echo " $0 -help"
 exit 1
fi


### Configuration 
echo "*** Installing cron script "

cat > /etc/cron.daily/myproxyDelegation.cron <<EOF

if ! [ $PROXY_PASSW ]; then
 echo "PROXY_PASSW not specified. voms-proxy cannot be created"
 exit 1
fi

echo $PROXY_PASSW | voms-proxy-init -cert /etc/grid-security/hostcert.pem \
    -key /etc/grid-security/hostkey.pem -voms cms -out $crabhome/server_proxy \
    -pwstdin -valid 13:00  

if [ $? -ne 0 ]; then
    echo " Unable to create VOMS proxy for the server"
    exit 1
fi

chown $crabuser $crabhome/server_proxy
chmod 700 $crabhome/server_proxy

myproxy-init -d -n -s $MYPROXY_SERVER --cert $crabhome/server_proxy --key $crabhome/server_proxy

EOF

chmod 700 /etc/cron.daily/myproxyDelegation.cron

echo "*** Installation completed"
echo ""
if ! [[ -e /etc/grid-security/hostkey.pem && -e /etc/grid-security/hostcert.pem ]]; then 
    ls /etc/grid-security/hostkey.pem /etc/grid-security/hostcert.pem
    echo " ==> Please remember to copy the machine certificate to the /etc/grid-security/ directory"
    echo "     with the correct permission settings "
    echo "     namely hostkey.pem must be ONLY READABLE and ONLY FROM USER (root)"
    echo ""
fi

}
################
### Unistall and cleanup
################
uninstall(){

echo "*** Cleanup cron script file"

rm -f /etc/cron.daily/myproxyDelegation.cron

}
#####################################

case $command in
    install )
        install ;;
    uninstall )
        uninstall ;;
esac
exit 0

