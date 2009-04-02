#!/bin/sh

#defaults
CRAB_HOME="/home/crab"
CRAB_USER="crab"
CRAB_GROUP="cms"

if [ $# -eq 0 ]; then
    echo " At least one option is needed. For usage see :"
    echo " $0 -help"
    exit 1
fi

while [ $# -gt 0 ]; do
case $1 in
        install )
          command=install
          shift ;;
        uninstall )
          command=uninstall
          shift;;
        -path )
          [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }           
          myarea="$2"
          shift; shift ;;
        -user )
          [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }
          CRAB_USER="$2"
          shift; shift ;;
        -homeuser )
          [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }
          CRAB_HOME="$2"
          shift; shift ;;
        -groupuser )
          [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }
          CRAB_GROUP="$2"
          shift; shift ;;
        -help )
          cat << \EOF_HELP

DelegationRPMs.sh 
A script to install/uninstall and configure the Delegation service ("root" permissions are required)

* Installation Syntax:
DelegationRPMs.sh install -path </your/dir> [-user <crabuser>] [-homeuser <crabuser Home dir>]

-path </your/dir>            : location of where the installation must be done 
-user <crabuser>             : user the delegation service will run (default: crab)
-homeuser <home dir of user> : home dir of user (default: /home/crab)

* Uninstall Syntax:

DelegationRPMs.sh uninstall -path </your/dir>


EOF_HELP
        exit 1
        ;;

        * )
            echo "$0: argument $1 not supported"; exit 1;;
    esac
done

if [ `whoami` != 'root' ]; then
    echo "You must to be root in order to do rpm install."
    echo "Exiting. "
    exit 1;
fi

##################
### Installation & Configuration
##################
install(){


if ! [ $myarea ]; then
 echo ""
 echo " -path option is missing. For usage see :"
 echo " $0 -help"
 exit 1
fi

if ! [ -d $CRAB_HOME ];then
 echo ""
 echo " $CRAB_HOME dir do not exist"
 echo " Specify the real $CRAB_USER home directory with -homeuser option."
 echo " $0 -help"
 exit 1
fi

mkdir -p $myarea
export MYTESTAREA=`readlink -f $myarea`;
mkdir -p $MYTESTAREA/RPMs

wget -nv -O ${MYTESTAREA}/CA_RPM_list.html http://glitesoft.cern.ch/LCG-CAs/current/
# CA_RPM_list=`grep RPMS.production ${MYTESTAREA}/CA_RPM_list.html | cut -c 27- | grep -Eo .*\.rpm\> | rev | cut -c 2- | rev `;
CA_RPM_list=`grep rpm ${MYTESTAREA}/CA_RPM_list.html | sed 's/.*[^A-Za-z0-9\.\_\-]\(.*\.rpm\)[^A-Za-z0-9\.\_\-].*/\1/'`;
rm -f ${MYTESTAREA}/CA_RPM_list.html

LCG_RPM_list="lcg-vomscerts-5.4.0-1.noarch.rpm"
VDT_RPM_list="vdt_globus_essentials-VDT1.6.0x86_rhas_4-1.i386.rpm myproxy-VDT1.6.0x86_rhas_4-1.i386.rpm"
API2_RPM_list="glite-security-voms-api-c-1.8.3-4.slc4.i386.rpm"
#API_RPM_list="glite-security-voms-api-c-1.7.16-2.slc4.i386.rpm gridsite-shared-1.1.18.1-1.i386.rpm"
API_RPM_list="gridsite-shared-1.1.18.1-1.i386.rpm"

#PROXY_RPM_list="glite-security-proxyrenewal-1.3.4-2.slc4.i386.rpm"
PROXY_RPM_list="glite-security-proxyrenewal-1.3.5-1.slc4.i386.rpm"
#PROXY_RPM_list="glite-security-proxyrenewal-1.3.6-1.slc4.i386.rpm"

GRIDSITEDEV_RPM_list="gridsite-devel-1.1.18.1-1.i386.rpm"
# ASAP_RPM_list="asap-delegation-server-edg-0.5.1-rpm"
ASAP_RPM_list="asap-delegation-server-edg-0.5.1-1.noarch.rpm"

echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $CA_RPM_list 
for arpm in $CA_RPM_list; do
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://linuxsoft.cern.ch/LCG-CAs/current/RPMS.production/$arpm
done
echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $LCG_RPM_list;
for arpm in $LCG_RPM_list; do
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/generic/sl4/i386/RPMS.updates/$arpm
done

echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $VDT_RPM_list;
for arpm in $VDT_RPM_list; do
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/generic/sl4/i386/RPMS.externals/$arpm
done

echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $API2_RPM_list;
for arpm in $API2_RPM_list; do
#    wget -nv -O $MYTESTAREA/RPMs/$arpm http://grid-it.cnaf.infn.it/mrepo/glite-cert_sl4-x86_64/RPMS.wn-updates/$arpm
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://linuxsoft.cern.ch/EGEE/gLite/R3.1/glite-WMS/sl4/i386/RPMS.updates/$arpm
done

echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $API_RPM_list;
for arpm in $API_RPM_list; do
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/generic/sl4/i386/RPMS.release/$arpm
done



echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $PROXY_RPM_list;
for arpm in $PROXY_RPM_list; do
#    wget -nv -O $MYTESTAREA/RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.glite.security.proxyrenewal/1.3.4/slc4_ia32_gcc346/$arpm
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.glite.security.proxyrenewal/1.3.5/slc4_ia32_gcc346/$arpm
#    wget -nv -O $MYTESTAREA/RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.glite.security.proxyrenewal/1.3.6/slc4_ia32_gcc346/$arpm
done

echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $GRIDSITEDEV_RPM_list;
for arpm in $GRIDSITEDEV_RPM_list; do
    wget -nv -O $MYTESTAREA/RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.gridsite.core/1.1.18/slc4_ia32_gcc346/$arpm
done

echo "*** Downloading to $MYTESTAREA/RPMs the RPMs :"; echo $ASAP_RPM_list;
for arpm in $ASAP_RPM_list; do
    wget -nv --no-check-certificate -O $MYTESTAREA/RPMs/$arpm https://cmsweb.cern.ch/crabconf/$arpm
done


List="$CA_RPM_list $LCG_RPM_list $VDT_RPM_list $API2_RPM_list $API_RPM_list $ASAP_RPM_list $PROXY_RPM_list $GRIDSITEDEV_RPM_list"
RPMList="";
echo "*** Checking already installed RPMs (it may takes some time...)";
rpm -qa > ${MYTESTAREA}/PresentRPM.list
echo -n > ${MYTESTAREA}/WantedRPM.list
for arpm in $List; do
    echo $arpm >> ${MYTESTAREA}/WantedRPM.list
#    RPMList="$RPMList $MYTESTAREA/RPMs/$arpm "
done

RPMList=$(echo `cat ${MYTESTAREA}/WantedRPM.list | grep -vFf ${MYTESTAREA}/PresentRPM.list | awk '{print PATH $1}' PATH=$MYTESTAREA\/RPMs\/`);

rm -f ${MYTESTAREA}/PresentRPM.list
rm -f ${MYTESTAREA}/WantedRPM.list

if [ -n "$RPMList" ]; then
    echo "*** Installing "
    rpm -Uvh $RPMList
    rpmresult=$?
    echo "rpm -Uvh $RPMList"
    if [[ $rpmresult -ne 0 ]];then
        echo "===> RPM installation failed, exit code $rpmresult. Exiting..."
        exit 1
    fi
else
    echo "*** No packages to be installed. "
fi

### Configuration 

echo "*** Configuring /etc/glite.conf ";
if [ -e /etc/glite.conf ];then
 echo "Overwriting the existing /etc/glite.conf file"
fi
cat > /etc/glite.conf << EOF
GLITE_USER=${CRAB_USER}
GLITE_HOST_CERT=${CRAB_HOME}/.globus/hostcert.pem
GLITE_HOST_KEY=${CRAB_HOME}/.globus/hostkey.pem
GLITE_CERT_DIR=/etc/grid-security/certificates
X509_VOMS_DIR=/etc/grid-security/vomsdir
EOF


echo "*** Creating proxydelegation profiles in /etc/profile.d"; 
cat > /etc/profile.d/proxydelegation.sh <<EOF
PATH_TO_ADD="/opt/globus/bin /opt/glite/bin";
for path_to_add in \$PATH_TO_ADD; do
   if ! echo \${PATH} | grep -q \${path_to_add} ; then
      PATH=\${path_to_add}:\${PATH}
   fi
done
PATH_TO_ADD="/opt/globus/lib /opt/glite/lib";
for path_to_add in \$PATH_TO_ADD; do
   if ! echo \${LD_LIBRARY_PATH} | grep -q \${path_to_add} ; then
      LD_LIBRARY_PATH=\${path_to_add}:\${LD_LIBRARY_PATH}
   fi
done
EOF

cat > /etc/profile.d/proxydelegation.csh <<EOF
foreach path_to_add ( /opt/globus/bin /opt/glite/bin )
   if !(\$?path) then
        set path = ( \${path_to_add} )
   else if ( "\${path}" !~ *\${path_to_add}* ) then
        set path = ( \${path_to_add}:\$path )
   endif
end
foreach path_to_add ( /opt/globus/lib /opt/glite/lib )
   if !(\$?LD_LIBRARY_PATH) then
        set LD_LIBRARY_PATH = ( \${path_to_add} )
   else if ( "\${LD_LIBRARY_PATH}" !~ *\${path_to_add}* ) then
        set LD_LIBRARY_PATH = ( \${path_to_add}:\$LD_LIBRARY_PATH )
   endif
end
EOF

echo "*** Configuring /opt/asap/etc/delegation.conf for user $CRAB_USER";
less /opt/asap/etc/delegation.conf | sed -e "s?asapuser?$CRAB_USER?g" > delegationtmp.conf
mv delegationtmp.conf /opt/asap/etc/delegation.conf


echo "*** run ldconfig";
cat > /etc/ld.so.conf.d/globusglite.conf << EOF
/opt/glite/lib
/opt/globus/lib
EOF
ldconfig

echo "*** Creating /var/www/html/transfer for user $CRAB_USER:$CRAB_GROUP";
mkdir -p /var/www/html/transfer
chown -R $CRAB_USER:$CRAB_GROUP /var/www/html/transfer

echo ""
echo "*** Note:"
echo " ==> If not already done, please remember to copy the machine certificate to the ${CRAB_HOME}/.globus directory "
echo "     with the correct permission settings "
echo ""
}
################
### Unistall and cleanup
################
uninstall(){

if ! [ $myarea ]; then
 echo ""
 echo " -path option is missing. For usage see :"
 echo " $0 -help"
 exit 1
fi

export MYTESTAREA=`readlink -f $myarea`;
UnInstallList=""
for arpm in `ls $MYTESTAREA/RPMs/*.rpm`; do
    barerpm=`echo $arpm | rev | cut -c 5- | rev`
    barerpm=`basename $barerpm`
    UnInstallList="$UnInstallList $barerpm";
done

echo "*** UnInstalling "
rpm -ev $UnInstallList
unresult=$?
echo "rpm -ev $UnInstallList"
if [[ $unresult -ne 0 ]]; then
   echo "===> RPM uninstall failed. Exiting..."
   exit 1
fi
echo "*** Remove $MYTESTAREA/RPMs"
rm -rf $MYTESTAREA/RPMs
echo "*** Cleanup configuration files"
if [ -e /opt/asap/etc/delegation.conf ] ;then
 rm -f /opt/asap/etc/delegation.conf 
fi
if [ -e /etc/glite.conf ] ;then
 rm -f /etc/glite.conf
fi
if [ -e /etc/profile.d/proxydelegation.sh ]; then
 rm -f /etc/profile.d/proxydelegation.sh
fi
if [ -e /etc/profile.d/proxydelegation.csh ]; then
 rm -f /etc/profile.d/proxydelegation.csh
fi
 

}
#####################################
case $command in
    install )
        install ;;
    uninstall )
        uninstall ;;
esac
exit 0

