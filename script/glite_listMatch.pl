#! /usr/bin/perl
#
$|=1;
#
# ------------------- Get info on local environment --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# ------------------- Optional logging of submission -------------------------
#  Log file 
$len=@ARGV;
if ( $len == 1 ) {
    $logFile = $ARGV[0];
    open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
    print LOG "\n\t************************************************\n\n";
    print LOG "retrieving availbale sites from user requirements\n";
}
#
# ----------------------------------- Main  -----------------------------------
%classad = &parseClassAd($cladfile);
if ( %classad ) {
    &processClassAd();
    &listMatch();
} elsif (LOG) {
    print LOG "Error: passed empty classad\n\n";
}
#
# ----------------------------- End of main ----------------------------------
#
# ----------------------- General pourpose routines --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# Read a file containing a classad and store it in a hash
sub parseClassAd {
    my $cladfile=$_[0];
    my $cladstring="";
    while ( <STDIN> ) {
#	if (LOG) {
#	    print LOG "ClassAd line: $_";
#	}
	$line = $_;
	chomp($line);
	$cladstring.=$line;
    }
    close(CLAD);
    if ( $cladstring =~ /.*\[(.+)\].*/ ) {
	$cladstring=$1;
    }
#    print LOG "$cladstring\n";
    my @attribs = split(/;/,$cladstring);
    foreach $attrib (@attribs) {
#	print LOG "$attrib\n";
	if ( $attrib =~ /\s*(\w+)\s*=\s*(.+)/ ) {
	    $clad{$1}=$2;
#	    print LOG "In hash: $1 = $clad{$1} \n";
	}
    }
    return %clad;
}
#
# Process a single entry of the classad
sub processClassAd {
    $rbconfigstring ="";
    $rbconfigVO="";
    $append2JDL = "";
    
    foreach $ClAdKey ( keys %classad ) {
#	print LOG "in hash2: $ClAdKey = $classad{$ClAdKey}\n";
	$ClAdVal = $classad{$ClAdKey};
#	    print LOG "$ClAdKey = $ClAdVal;\n";
	if ( $ClAdKey eq "WMSconfig") {
	    $ClAdVal =~ s/\s*{\s*\"\s*(.*)\s*\"\s*}\.*/$1/;
#               print "$ClAdKey : $ClAdVal \n";                
	    chomp($ClAdVal);
	    $rbconfigstring="-c $ClAdVal";
#               print "$rbconfigstring \n";
	} elsif ( $ClAdKey ne "" ) {
	    $append2JDL.="$ClAdKey = $ClAdVal;\n";
	}
    }
}
#
# Execute list match command
sub listMatch {
    # open a temporary file
    $tmpfile = `mktemp glite_XXXXXX` || die "error";
    chomp($tmpfile);
    open (JDL, ">$tmpfile") || die "error";
    $append2JDL.="Executable    = \"/bin/echo\";\n";
    print JDL $append2JDL;
    close(JDL);
    # put jdl in the log file
    if (LOG) {
	print LOG "user JDL: [\n";
	print LOG $append2JDL;
	print LOG ("]\n\n");
    }
    # submitting command
    $subcmd = "glite-wms-job-list-match -a --noint $rbconfigstring $tmpfile 2>&1|";
#    print $subcmd;
    print LOG "subcmd = $subcmd\n";
    # exit;
    # open a pipe to read the stdout of edg-job-submit
    open (SUB, $subcmd);
    while ( <SUB> ) {
	if ( $_ =~ m/\s*CEId*/) {
	    $getid=1;
	} elsif ( $getid==1 ) {
	    if ( $_ =~ m/\s-\s(.+)/  ) {
		print "$1\n";
	    }
	}
	print LOG $_;
    }

    unlink "$tmpfile";
}
#
# ----------------------------------------------------------------------------


