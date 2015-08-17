#!/usr/bin/perl

use strict;
use warnings;

use File::Copy qw(move);

my @files = @ARGV;
my $unique_filename = "/tmp/RemoveDuplicateJobAds_tmp";
my $master_ref = {};
my $file_ref;
my $unique_text;
my $fh;
my $unique_ref;
my $filename;

print_log("sort filenames");
@files = sort @files;

foreach $filename (@files)
{
	print_log("parse $filename");
	$file_ref = parse_jobads($filename);
	
	print_log("compare $filename to master");
	$unique_text = compare_jobads($master_ref, $file_ref);

	print_log("write unique jobads to $unique_filename");
	open($fh, ">", $unique_filename) or die "cannot open > $unique_filename: $!";
	print $fh $unique_text;
	close $fh;

	$unique_ref = parse_jobads($unique_filename);
	@$master_ref{ keys %$unique_ref } = values %$unique_ref;
	print_log("master " . scalar(keys %$master_ref)); 
	
	print_log("move $unique_filename to $filename");	
	move $unique_filename, $filename;	
} 

sub print_log
{
	print localtime() . " - " . $_[0] . "\n"; 
}

sub compare_jobads
{
	my ($master_ref, $file_ref) = @_;
	my $output = "";

	foreach my $file_key (keys %$file_ref)
	{
		if (! exists $master_ref->{$file_key})
		{
			$output .= print_jobad($file_ref->{$file_key});
		}
	}

	return $output;
}

sub parse_jobads 
{
	my $filename = shift;
	my $jobads_ref = {};
	my $jobad_ref = {};
	open (FILE, '<', $filename) or die "Could not open $filename: $!";
	while (<FILE>) 
	{
        	if ($_ =~ /(\w*)\s*=\s*(.*)/)
		{
			$jobad_ref->{$1} = $2;
		}
		elsif ($_ eq "\n")
		{
			$jobads_ref->{$jobad_ref->{"GlobalJobId"}} = $jobad_ref;
			$jobad_ref = {};
		}
		else
		{
			die "regex fail '$_'\n";
		}
    	}
    	close (FILE) or die "Could not close $filename: $!";

	return $jobads_ref;

}

exit 0;

sub print_all_jobads
{
	my ($jobads_ref) = @_;
	my $output = "";

	foreach my $jobads_key (keys %$jobads_ref){
		$output .= print_jobad($jobads_ref->{$jobads_key});
	}

	return $output;
}

sub print_jobad
{
	my ($jobad_ref) = @_;
	my $output = "";

        foreach my $h (keys %$jobad_ref){
	        $output .=  $h . " = " . $jobad_ref->{$h} . "\n";
        }
        $output .= "\n";

	return $output;
}

