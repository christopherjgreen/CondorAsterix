#!/usr/bin/perl

use strict;
use warnings;

require 'LoadJobAds.pl';

use CondorAsterix;

my @files = </opt/jobAds/jobAds_1438791969>;
my $backup_filename;
my $file;


exit 1;

foreach $backup_filename (@files) {
	# convert job ads to ADM
	print localtime() . " Moving $backup_filename to adm file in working directory.\n";
	open($file, '>', $adm_filename) or die "Unable to open file '$adm_filename' $!";
	$output = `cat $backup_filename | ./ClassAdToJson.pl`;
	print $file $output;
	close $file;

	# move data from adm file to raw dataset
	print localtime() . " Inserting data into RawCondorJobAds.\n";
	run_query($asterixdb_url . "ddl?ddl=". uri_escape $raw_job_ads_insert);
	
	# move raw data into JobAds dataset
	print localtime() . " Inserting Raw data into JobAds dataset.\n";
	run_query($asterixdb_url . "update?statements=" . uri_escape $job_ads_insert);
	
	print localtime() . " $backup_filename has been loaded.\n";
}


# subroutine to run a query on the Asterix DB
#sub run_query
#{
#        my $url = $_[0];
#        my $ua = LWP::UserAgent->new();
#        my $response = $ua->get($url);
#        if (! $response->is_success) {
#                die "Unable to get $url\n" .
#                $response->status_line . "\n" .
#                $response->decoded_content . "\n";
#        }
#        return $response->decoded_content;
#}


