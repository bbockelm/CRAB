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
SLVER=5

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
        -sl )
            SLVER="$2"
            shift; shift ;;        
        -help )
            cat << \EOF_HELP

GridFTPrpms.sh 
A script to install/uninstall and configure the GridFTP service.

* Installation Syntax:

GridFTPinstall_1_1_X.sh install -path </your/dir> [-sl <ver>] 

-path </your/dir>            : location of where the installation must be done 
-sl <ver>                    : Scientific Linux version (default: 5)

* Uninstall Syntax:

GridFTPinstall_1_1_X.sh uninstall -path </your/dir>

EOF_HELP
            exit 1
            ;;
        * )
            echo "$0: argument $1 not supported"; exit 1;;
    esac
done

##################
### Packages 
##################

LCGCA_RPM="lcg-CA"

GRIDFTP_RPM="
glite-initscript-globus-gridftp
"

# don't know if the these are actually needed, but Daniele highlighted some 
# missing dependencies with GSI tunneling libs. Then I make'em explicit.
# TODO Remove once we are sure they are unnecessary
# excluded (as in UI repo, not in CREAM ones): vdt_globus_sdk
EXPLICIT_DEPS_RPM="
fetch-crl
gpt
vdt_globus_data_server
vdt_globus_essentials
glite-security-lcas-interface
glite-security-lcas
glite-security-lcmaps
"
GRIDFTP_RPM="$GRIDFTP_RPM $EXPLICIT_DEPS_RPM"

LCAS_LCMAPS_RPM="
glite-security-lcas-lcmaps-gt4-interface
glite-security-lcas-plugins-voms glite-security-lcas-plugins-basic glite-security-lcas-plugins-check-executable
glite-security-lcmaps-plugins-basic glite-security-lcmaps-plugins-voms glite-security-lcmaps-plugins-verify-proxy
"

VOMS_RPM="glite-security-voms-api-c glite-security-voms-api-cpp"

GRIDSITE_RPM="gridsite-shared"

### List of Packages used as YUM arguments

INSTALL_PACKLIST="$LCGCA_RPM $GRIDFTP_RPM $LCAS_LCMAPS_RPM $VOMS_RPM $GRIDSITE_RPM"
UNINSTALL_PACKLIST="vdt_globus_data_server glite-security-lcas glite-security-lcmaps"

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
YUM_REPO="http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.2"
if [ $SLVER -eq 4 ]; then
    echo "*** Sourcing repositories for Scientific Linux 4"
    YUM_REPO="http://grid-deployment.web.cern.ch/grid-deployment/glite/repos/3.1"
fi

if ! [ -e /etc/yum.repos.d/lcg-CA.repo ]; then
    echo "Downloading lcg-CA.repo into /etc/yum.repos.d/lcg-CA.repo"
    wget -O /etc/yum.repos.d/lcg-CA.repo $YUM_REPO/lcg-CA.repo
fi

if ! [ -e /etc/yum.repos.d/glite-CREAM.repo ]; then
    echo "Downloading glite-CREAM.repo into /etc/yum.repos.d/glite-CREAM.repo"
    wget -O /etc/yum.repos.d/glite-CREAM.repo $YUM_REPO/glite-CREAM.repo
fi

### install packages
echo "*** Installing packages :";
if ! yum $YUMOPTIONS $INSTALL_PACKLIST 2>&1; then
    echo Exiting $0
    exit 1
fi

#CMS_SERVER="http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab"
CMS_SERVER="http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/Repository/"
# "https://cmsweb.cern.ch/crabconf"

mkdir -p $MYTESTAREA/GFTP_CFGfiles
echo "*** Downloading to $MYTESTAREA/GFTP_CFGfiles defaults config files tarball"
if ! wget --user-agent='' --no-check-certificate -O $MYTESTAREA/GFTP_CFGfiles/GridFTPinstall.tar.gz $CMS_SERVER/GridFTPinstall.tar.gz; then
    echo Exiting from $0
    exit 1
fi

echo "*** Untarring to $MYTESTAREA/GFTP_CFGfiles defaults config files tarball: "
if ! tar -C $MYTESTAREA/GFTP_CFGfiles/ -xzvf $MYTESTAREA/GFTP_CFGfiles/GridFTPinstall.tar.gz; then
    echo Exiting from $0
    exit 1
fi

### Configuration 
echo "*** Creating cmsXXX local users: "
/usr/sbin/groupadd cms
mkdir -p /etc/grid-security/gridmapdir/

for i in {0..9}; do
    for j in {0..9}; do
        for k in {0..9}; do
            id=$i$j$k
            echo -n "cms$id "
            echo "cms$id:x:1$id:cms::/home/cms$id:/bin/bash" >> $MYTESTAREA/GFTP_CFGfiles/cmsnewusers
            # /usr/sbin/useradd -g cms -m cms$id
            touch /etc/grid-security/gridmapdir/cms$id
        done
    done
done
/usr/sbin/newusers $MYTESTAREA/GFTP_CFGfiles/cmsnewusers
echo ""


echo "*** Configuring LCAS-LCMAPS"
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

echo "*** Creating mapfiles"
#/etc/grid-security/grid-mapfile
if ! [ -e /etc/grid-security/grid-mapfile ]; then
    if cp $MYTESTAREA/GFTP_CFGfiles/grid-mapfile /etc/grid-security/; then
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

# NOTE: by removing a pkg all the pkgs relying on i will be automatically removed 
YUMOPTIONS="remove "
 
if ! yum $YUMOPTIONS $UNINSTALL_PACKLIST 2>&1; then
    echo "===> Yum package removal failed. Exiting ..." 
    exit 1
fi

# extract group-cms users:
echo "*** Removing cmsXXX user"; echo -n "found users: "
if [ -e $MYTESTAREA/GFTP_CFGfiles/cmsnewusers ]; then
    USER_LIST=`cat $MYTESTAREA/GFTP_CFGfiles/cmsnewusers | awk -F\: '{print $1}'`
else
    USER_LIST=`cat /etc/passwd | awk -F\: '{print $1 }'  | xargs groups | awk -F\: '{print $1 }' | grep ^cms`
fi

for uu in $USER_LIST; do 
    echo -n "$uu "
    userdel -r $uu 
    rm -f /etc/grid-security/gridmapdir/$uu 
done 

echo 
echo "*** Removing LCAS/LCMAPS configuration files"
rm -rf /opt/glite/etc/lcas/lcas.db
rm -rf /opt/glite/etc/lcmaps/lcmaps.db
echo

echo "*** Please remove $MYTESTAREA/GFTP_CFGfiles"
echo 
echo "*** Uninstall completed"
}
#####################################
case $command in
    install )
        install ;;
    uninstall )
        uninstall ;;
    * )
        echo " unrecognized command. For usage see :"
        echo " $0 -help"
        exit 1
esac
exit 0

