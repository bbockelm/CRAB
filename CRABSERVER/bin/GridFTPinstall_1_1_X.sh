#!/bin/bash

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

#defaults

SO_FAMILY="4"
ARCH_FAMILY="ia32"

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
        -so )
            [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }
            SO_FAMILY="$2"
            shift; shift ;;
        -arch )
            [ $# -gt 1 ] || { echo "Option \`$1' requires an argument" 1>&2; exit 1;  }
            ARCH_FAMILY="$2"
            shift; shift ;;
        -help )
            cat << \EOF_HELP

GridFTPrpms.sh 
A script to install/uninstall and configure the GridFTP service.

* Installation Syntax:

GridFTPrpms.sh install -path </your/dir> [-so <SL(C) ver>] [-arch <your arch>]

-path </your/dir>            : location of where the installation must be done 

-so <SL version>             : version for the used OS, allowed values are 4 or 5 [default 4]

-arch <your architecture>    : adopted architecture, allowed values are ia32 or x86_64 [default ia32]   

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
cd $MYTESTAREA

YUMOPTIONS="install -y"

### check if yum is installed
if ! which yum; then
    echo
    echo Unable to locate Yum package manager. Exiting $0
    exit 1
fi

echo "Yum options: "$YUMOPTIONS

### check if yum has the right repositories
YUM_REPO="http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.1";
if [ $SO_FAMILY == "5" ]; then 
    YUM_REPO="http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2"
fi

if ! [ -e /etc/yum.repos.d/lcg-CA.repo ]; then
    echo "Downloading lcg-CA.repo into /etc/yum.repos.d/lcg-CA.repo"
    wget -O /etc/yum.repos.d/lcg-CA.repo $YUM_REPO/lcg-CA.repo
fi

if ! [ -e /etc/yum.repos.d/glite-UI.repo ]; then
    echo "Downloading glite-UI.repo into /etc/yum.repos.d/glite-UI.repo"
    wget -O /etc/yum.repos.d/glite-UI.repo $YUM_REPO/glite-UI.repo
fi

### install packages

echo "*** Installing CAs and VOMS certificates :";
if ! yum $YUMOPTIONS lcg-CA lcg-vomscerts 2>&1; then
    echo Exiting $0
    exit 1
fi

echo "*** Installing GridFtp essentials :";
if ! yum $YUMOPTIONS glite-initscript-globus-gridftp 2>&1; then
    echo Exiting $0
    exit 1
fi

echo "*** Installing GT4 interface for lcas-lcmaps, LCAS plugins, LCMAPS plugins :";
PACKLIST="
glite-security-lcas-lcmaps-gt4-interface
glite-security-lcas-plugins-voms glite-security-lcas-plugins-check-executable 
glite-security-lcmaps-plugins-basic glite-security-lcmaps-plugins-voms glite-security-lcmaps-plugins-verify-proxy
"

if ! yum $YUMOPTIONS $PACKLIST 2>&1; then
    echo Exiting $0
    exit 1
fi

echo "*** Installing VOMS APIs :";
if ! yum $YUMOPTIONS glite-security-voms-api-c glite-security-voms-api-cpp 2>&1; then
    echo Exiting $0
    exit 1
fi

# download specific RPMs not in repositories and install

echo "*** Installing Gridsite related :"; 
GRIDSITE_RPM="gridsite-shared-1.1.18.1-1.i386.rpm"
GRIDSITEDEV_RPM="gridsite-devel-1.1.18.1-1.i386.rpm"
GRIDSITE_REPO="http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.gridsite.core/1.1.18/slc4_ia32_gcc346/"

# for SLC5 and ia32
if [ $SO_FAMILY == "5" ] && [ $ARCH_FAMILY == "ia32" ] ; then
    GRIDSITE_RPM="gridsite-shared-1.5.10-1.i386.rpm"
    GRIDSITEDEV_RPM="gridsite-devel-1.5.10-1.i386.rpm"
    GRIDSITE_REPO="http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.gridsite.core/1.5.10/sl5_ia32_gcc412/"
fi

# for SLC5 and x86_64
if [ $SO_FAMILY == "5" ] && [ $ARCH_FAMILY == "x86_64" ] ; then 
    GRIDSITE_RPM="gridsite-shared-1.5.10-1.sl5.x86_64.rpm"
    GRIDSITEDEV_RPM="gridsite-devel-1.5.10-1.x86_64.rpm"
    GRIDSITE_REPO="http://eticssoft.web.cern.ch/eticssoft/repository/org.glite/org.gridsite.core/1.5.10/sl5_x86_64_gcc412/"
fi

echo "***\t Downloading to $MYTESTAREA/GFTP_RPMs the RPMs :" $GRIDSITE_RPM $GRIDSITEDEV_RPM;
if ! wget -nv -O $MYTESTAREA/GFTP_RPMs/$GRIDSITE_RPM $GRIDSITE_REPO$GRIDSITE_RPM; then
    echo Exiting from $0
    exit 1
fi

if ! wget -nv -O $MYTESTAREA/GFTP_RPMs/$GRIDSITEDEV_RPM $GRIDSITE_REPO$GRIDSITEDEV_RPM; then
    echo Exiting from $0
    exit 1
fi

echo "***\t Installing " $GRIDSITE_RPM $GRIDSITEDEV_RPM;
rpm -Uvh --force $MYTESTAREA/GFTP_RPMs/$GRIDSITE_RPM $MYTESTAREA/GFTP_RPMs/$GRIDSITEDEV_RPM  2>&1  
rpmresult=$?
if [[ $rpmresult -ne 0 ]];then
    echo "===> RPM installation failed, exit code $rpmresult. Exiting..."
    exit 1
fi

# old address: http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/GridFTPinstall.tar.gz
#CMS_SERVER="http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab"
CMS_SERVER="https://cmsweb.cern.ch/crabconf"

mkdir -p $MYTESTAREA/GFTP_CFGfiles
echo "*** Downloading to $MYTESTAREA/GFTP_CFGfiles defaults config files tarball"
if ! wget --no-check-certificate -O $MYTESTAREA/GFTP_CFGfiles/GridFTPinstall.tar.gz $CMS_SERVER/GridFTPinstall.tar.gz; then
    echo Exiting from $0
    exit 1
fi

echo "*** Untarring to $MYTESTAREA/GFTP_CFGfiles defaults config files tarball: "
if ! tar -C $MYTESTAREA/GFTP_CFGfiles/ -xzvf $MYTESTAREA/GFTP_CFGfiles/GridFTPinstall.tar.gz; then
    echo Exiting from $0
    exit 1
fi

### Configuration 
echo "*** Installing fetch-certificates script "
mkdir -p /etc/grid-security/certificates
chmod +r /etc/grid-security/certificates

echo "*** Installing fetch-certificates wrapper "
mkdir -p /opt/glite/libexec/
cat > /opt/glite/libexec/fetch-crl.sh <<EOF
#       Set default value if not known
yum update lcg-CA
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
if ! [ -e /etc/grid-security/hostkey.pem ] && [ -e /etc/grid-security/hostcert.pem ]; then 
    ls /etc/grid-security/hostkey.pem /etc/grid-security/hostcert.pem
    echo " ==> Please remember to copy the machine certificate to the /etc/grid-security/ directory"
    echo "     with the correct permission settings "
    echo "     namley hostkey.pem must be ONLY READABLE and ONLY FROM USER (root)"
    echo ""
    echo " ==> Remember to copy the certificates also in .globus/ directory for the user running the server"
    echo "     otherwise you won't be able to delegate proxies for users"
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

echo "*** Removing specific RPMs"
export MYTESTAREA=`readlink -f $myarea`;
UnInstallList=""
for arpm in `ls $MYTESTAREA/GFTP_RPMs/*.rpm`; do
    barerpm=`echo $arpm | rev | cut -c 5- | rev`
    barerpm=`basename $barerpm`
    UnInstallList="$UnInstallList $barerpm";
done

echo "*** Removing Yum packages"
echo "\t NOTE: lcg-CA lcg-vomscerts packages won't be removed"

YUMOPTIONS="remove -y"

# NOTE: dependencies are removed automatically.
#       that is, removing a base pkg all the depending pkgs will be removed too 
YUM_PKG_LIST="glite-security-voms-api-c glite-security-voms-api-cpp
vdt_globus_data_server glite-security-lcas-interface glite-security-lcmaps"

if ! yum $YUMOPTIONS $YUM_PKG_LIST 2>&1 > uninstall.log; then
    echo "===> Yum package removal failed. Exiting ..." 
    exit 1
fi


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

