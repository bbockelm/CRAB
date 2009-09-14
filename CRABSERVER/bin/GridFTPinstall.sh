#!/bin/sh

if [ `whoami` != 'root' ]; then
    echo "You must to be root in order to do rpm install."
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
            shift ;;
        uninstall )
            command=uninstall
            shift;;
        -path )
            [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }           
            myarea="$2"
            shift; shift ;;
        -help )
            cat << \EOF_HELP

GridFTPrpms.sh 
A script to install/uninstall and configure the GridFTP service.

* Installation Syntax:

GridFTPrpms.sh install -path </your/dir> [-user <crabuser>] [-homeuser <crabuser Home dir>]

-path </your/dir>            : location of where the installation must be done 

* Uninstall Syntax:

GridFTPrpms.sh uninstall -path </your/dir>


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

if ! [ $myarea ]; then
 echo ""
 echo " -path option is missing. For usage see :"
 echo " $0 -help"
 exit 1
fi

mkdir -p $myarea
export MYTESTAREA=`readlink -f $myarea`;
mkdir -p $MYTESTAREA/GFTP_RPMs

wget -nv -O ${MYTESTAREA}/CA_RPM_list.html http://glitesoft.cern.ch/LCG-CAs/current/
CA_RPM_list=`grep rpm ${MYTESTAREA}/CA_RPM_list.html | sed 's/.*[^A-Za-z0-9\.\_\-]\(.*\.rpm\)[^A-Za-z0-9\.\_\-].*/\1/'`;
rm -f ${MYTESTAREA}/CA_RPM_list.html

LCG_RPM_list="lcg-vomscerts-5.4.0-1.noarch.rpm"

GFTP_RPM_list="glite-initscript-globus-gridftp-1.0.0-1.noarch.rpm"
VDT_RPM_list="vdt_globus_data_server-VDT1.6.1x86_rhas_4-7.i386.rpm vdt_globus_essentials-VDT1.6.1x86_rhas_4-7.i386.rpm"; 
## VDT_RPM_list="vdt_globus_data_server-VDT1.6.1x86_rhas_4-3.i386.rpm ";

LCMAPS_RPM_list="glite-security-lcmaps-1.3.14-2.slc4.i386.rpm"
GLITE_U_RPM_list="glite-security-lcmaps-plugins-basic-1.3.7-2.slc4.i386.rpm glite-security-voms-api-cpp-1.8.3-3.slc4.i386.rpm glite-security-voms-api-c-1.8.3-4.slc4.i386.rpm glite-security-lcmaps-plugins-voms-1.3.7-2.slc4.i386.rpm glite-security-voms-clients-1.7.22-1.slc4.i386.rpm "
GLITE_R_RPM_list="glite-security-lcas-plugins-voms-1.3.3-1.slc4.i386.rpm glite-security-lcmaps-interface-1.3.14-1.slc4.i386.rpm glite-security-lcas-1.3.7-0.slc4.i386.rpm glite-security-lcas-interface-1.3.6-1.slc4.i386.rpm glite-security-lcas-plugins-check-executable-1.2.1-1.slc4.i386.rpm glite-security-lcmaps-plugins-verify-proxy-1.2.8-1.slc4.i386.rpm "

GT4_RPM_list="org.glite.security.lcas-lcmaps-gt4-interface-0.0.13-1.slc4.i386.rpm"

XALAN_RPM_list="xalan-c-1.10.0-1.slc4.i686.rpm"
LOG4_RPM_list="log4cpp-0.3.4b-1.slc4.i386.rpm"
XERCES_RPM_list="xerces-c-2.7.0-1.slc4.i686.rpm"

API_RPM_list="gridsite-shared-1.1.18.1-1.i386.rpm"
GRIDSITEDEV_RPM_list="gridsite-devel-1.1.18.1-1.i386.rpm"


echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $CA_RPM_list;
for arpm in $CA_RPM_list; do
    if ! wget -nv -O $MYTESTAREA/GFTP_RPMs/$arpm http://linuxsoft.cern.ch/LCG-CAs/current/RPMS.production/$arpm; then
        echo Exiting $0
        exit
    fi 
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $LCG_RPM_list;
for arpm in $LCG_RPM_list; do
    if ! wget -nv -O $MYTESTAREA/GFTP_RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/generic/sl4/i386/RPMS.updates/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $GFTP_RPM_list; 
for arpm in $GFTP_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://linuxsoft.cern.ch/EGEE/gLite/R3.1/glite-UI/sl4/i386/RPMS.release/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $VDT_RPM_list;
for arpm in $VDT_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://linuxsoft.cern.ch/EGEE/gLite/R3.1/glite-UI/sl4/i386/RPMS.externals/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $LCMAPS_RPM_list;
for arpm in $LCMAPS_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.glite.security.lcmaps/1.3.14/slc4_ia32_gcc346/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $GLITE_U_RPM_list;
for arpm in $GLITE_U_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/lcg-CE/sl4/i386/RPMS.updates/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $GLITE_R_RPM_list;
for arpm in $GLITE_R_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/lcg-CE/sl4/i386/RPMS.release/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $GT4_RPM_list;
for arpm in $GT4_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.glite.security.lcas-lcmaps-gt4-interface/0.0.13/slc4_ia32_gcc346/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $XALAN_RPM_list;
for arpm in $XALAN_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/externals/xalan-c/1.10.0/slc4_ia32_gcc346/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $LOG4_RPM_list;
for arpm in $LOG4_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/externals/log4cpp/0.3.4b/slc4_ia32_gcc346/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $XERCES_RPM_list;
for arpm in $XERCES_RPM_list; do
    if ! wget -O $MYTESTAREA/GFTP_RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/externals/xerces-c/2.7.0/slc4_ia32_gcc346/$arpm; then
        echo Exiting $0
        exit
    fi
done

echo "*** Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :"; echo $API_RPM_list;
for arpm in $API_RPM_list; do
    wget -nv -O $MYTESTAREA//GFTP_RPMs/$arpm http://glitesoft.cern.ch/EGEE/gLite/R3.1/generic/sl4/i386/RPMS.release/$arpm
done

echo "*** Downloading to $MYTESTAREA//GFTP_RPMs the RPMs :"; echo $GRIDSITEDEV_RPM_list;
for arpm in $GRIDSITEDEV_RPM_list; do
    wget -nv -O $MYTESTAREA//GFTP_RPMs/$arpm http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.gridsite.core/1.1.18/slc4_ia32_gcc346/$arpm
done




echo "*** Checking already installed RPMs (it may takes some time...)";
rpm -qa > ${MYTESTAREA}/PresentRPM.list
echo -n > ${MYTESTAREA}/WantedGFtpRPM.list
List="$CA_RPM_list $LCG_RPM_list $GFTP_RPM_list $VDT_RPM_list $LCMAPS_RPM_list $GLITE_U_RPM_list $GLITE_R_RPM_list $GT4_RPM_list $XALAN_RPM_list $LOG4_RPM_list $XERCES_RPM_list"
for arpm in $List; do
    echo $arpm >> ${MYTESTAREA}/WantedGFtpRPM.list
done

RPMList=$(echo `cat ${MYTESTAREA}/WantedGFtpRPM.list | grep -vFf ${MYTESTAREA}/PresentRPM.list | awk '{print PATH $1}' PATH=$MYTESTAREA\/GFTP_RPMs\/`);

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

# old address: http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/GridFTPinstall.tar.gz
mkdir -p $MYTESTAREA/GFTP_CFGfiles
echo "*** Downloading to $MYTESTAREA/GFTP_CFGfiles defaults config files tarball"
if ! wget --no-check-certificate -O $MYTESTAREA/GFTP_CFGfiles/GridFTPinstall.tar.gz https://cmsweb.cern.ch/crabconf/GridFTPinstall.tar.gz; then
    echo Exiting from $0
    exit
fi

echo "*** Untarring to $MYTESTAREA/GFTP_CFGfiles defaults config files tarball: "
if ! tar -C $MYTESTAREA/GFTP_CFGfiles/ -xzvf $MYTESTAREA/GFTP_CFGfiles/GridFTPinstall.tar.gz; then
    echo Exiting from $0
    exit
fi

### Configuration 
echo "*** Installing fetch-certificates script "
mkdir -p /etc/grid-security/certificates
chmod +r /etc/grid-security/certificates

if ! [ -e /usr/sbin/fetch-crl ]; then
    if cp $MYTESTAREA/GFTP_CFGfiles/fetch-crl /usr/sbin/fetch-crl; then
        chmod +x /usr/sbin/fetch-crl
        echo created /usr/sbin/fetch-crl; 
    fi
fi

echo "*** Installing fetch-certificates wrapper "
mkdir -p /opt/glite/libexec/
cat > /opt/glite/libexec/fetch-crl.sh <<EOF
#       Set default value if not known
CRLDIR=\${X509_CERT_DIR:-/etc/grid-security/certificates}
/usr/sbin/fetch-crl --loc \${CRLDIR} --out \${CRLDIR} --no-check-certificate
EOF
# make the script executable
chmod a+x /opt/glite/libexec/fetch-crl.sh

echo "*** Adding such script as a cron job "
cat > /etc/cron.d/fetch-crl <<EOF
PATH=/sbin:/bin:/usr/sbin:/usr/bin
2 5,11,17,23 * * * root /opt/glite/libexec/fetch-crl.sh >> /var/log/fetch-crl-cron.log 2>&1
EOF

echo "*** Creating cmsXXX local users: "
/usr/sbin/groupadd cms
for i in {0..9}; do
    for j in {0..9}; do
        for k in {0..9}; do
            echo -n "cms$i$j$k "
            /usr/sbin/useradd -g cms -m cms$i$j$k
        done
    done
done
echo;
mkdir -p /etc/grid-security/gridmapdir/
for i in {0..9}; do
  for j in {0..9}; do
      for k in {0..9}; do
          touch /etc/grid-security/gridmapdir/cms$i$j$k
      done
  done
done

if ! /opt/glite/sbin/gt4-interface-install.sh install; then
    echo  Exiting from $0
fi

#/opt/glite/etc/lcas/lcas.db
if ! [ -e /opt/glite/etc/lcas/lcas.db ]; then
    mkdir -p /opt/glite/etc/lcas;
    if cp $MYTESTAREA/GFTP_CFGfiles/lcas.db /opt/glite/etc/lcas/; then
        echo " *** /opt/glite/etc/lcas/lcas.db created";
    fi
fi

#/opt/glite/etc/lcmaps/lcmaps.db
if ! [ -e /opt/glite/etc/lcmaps/lcmaps.db ]; then
    mkdir -p /opt/glite/etc/lcmaps;
    if cp $MYTESTAREA/GFTP_CFGfiles/lcmaps.db /opt/glite/etc/lcmaps/; then
        echo "*** /opt/glite/etc/lcmaps/lcmaps.db created";
    fi
fi

#/etc/grid-security/grid-mapfile
if ! [ -e /etc/grid-security/grid-mapfile ]; then
    if cp $MYTESTAREA/GFTP_CFGfiles/grid-mapfile  /etc/grid-security/; then
        echo "*** /etc/grid-security/grid-mapfile created";
    fi
fi
#/etc/grid-security/groupmapfile
if ! [ -e /etc/grid-security/groupmapfile ]; then
    if cp $MYTESTAREA/GFTP_CFGfiles/groupmapfile /etc/grid-security/; then
        echo "*** /etc/grid-security/groupmapfile created";
    fi
fi

echo "*** GridFTP installation completed"
echo ""
if ! [[ -e /etc/grid-security/hostkey.pem && -e /etc/grid-security/hostcert.pem ]]; then 
    ls /etc/grid-security/hostkey.pem /etc/grid-security/hostcert.pem
    echo " ==> Please remember to copy the machine certificate to the /etc/grid-security/ directory"
    echo "     with the correct permission settings "
    echo "     namley hostkey.pem must be ONLY READABLE and ONLY FROM USER (root)"
    echo ""
fi

mv $MYTESTAREA/GFTP_CFGfiles/GridFTPenv.{c,}sh .
echo " ==> Export needed environment variables sourcing the following script:"
echo "     GridFTPenv.c|sh"
echo ""

echo " ==> Be sure the needed ports are open through the firewall:"
echo "     iptables -I INPUT -p TCP --dport 20000:25000 -m state --state NEW -j ACCEPT "
echo "     iptables -I INPUT -p TCP --dport 2811 -m state --state NEW -j ACCEPT "
echo "     iptables -I INPUT -p udp --dport 20000:25000 -j ACCEPT "
echo ""

mv $MYTESTAREA/GFTP_CFGfiles/globus-gridftp .
echo " ==> Start GridFTP server daemon through the following script:"
echo "     globus-gridftp start"
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
for arpm in `ls $MYTESTAREA/GFTP_RPMs/*.rpm`; do
    barerpm=`echo $arpm | rev | cut -c 5- | rev`
    barerpm=`basename $barerpm`
    UnInstallList="$UnInstallList $barerpm";
done

echo "*** UnInstalling "
rpm -ev $UnInstallList
unresult=$?
echo "rpm -ev $UnInstallList"
if [[ $rpmresult -ne 0 ]]; then
   echo "===> RPM uninstall failed, exit code: $rpmresult. Exiting..."
   exit 1
fi


# extract group-cms users:
echo "*** Removing cmsXXX user"; echo -n "found users: "
for uu in $(for u in `cat /etc/passwd | awk -F\: '{print $1 }'`; do groups $u | grep -E ^cms | awk -F\: '{print $1 }' ; done ); do echo -n "$uu "; userdel -r $uu; done; echo; 

#rm -f $MYTESTAREA/PresentRPM.list
#rm -f $MYTESTAREA/WantedGFtpRPM.list
echo "*** Please Remove $MYTESTAREA/GFTP_RPMs"
#rm -rf $MYTESTAREA/GFTP_RPMs
echo "*** Please Remove $MYTESTAREA/GFTP_CFGfiles"
#rm -rf $MYTESTAREA/GFTP_CFGfiles
echo "*** Cleanup configuration files"


}
#####################################
case $command in
    install )
        install ;;
    uninstall )
        uninstall ;;
esac
exit 0

