#!/usr/bin/perl

# Takes condor_q -l format as input, spits out json, in the following form
# {} around each ad, newline between ads
# "" around each attribute name
# : next
# Numerics and true/false are literals
# everything else is a string

print "{\n";

while (<>) {
	if (/^$/) { 
		# Hack so I don't need to remove the last trailing comma
		print "\"end\": true\n";
		print "}\n{\n";
		next;
	}

	@a = split;
	print "\"$a[0]\"";
	print ":";

	my $key = $a[0];

	shift @a;
	shift @a;

	$rhs = join(" ", @a);

	if($key eq "RemoteWallClockTime"){
		$rhs =~ /(\d+)/;
		print "$1,\n";
		next;
	}

	if ($rhs =~ /^["]/) {
		print $rhs,",\n";
		next;
	}

	if ($rhs =~ /^[0-9.e+\-]+$/) {
		print $rhs,",\n";
		next;
	}

	if ($rhs =~ /^(true|false)$/i) {
		print $rhs,",\n";
		next;
	}

	$rhs =~ s/"/\\"/g;

	print "\"$rhs\"\n";
}
print "}\n";

exit(0);
