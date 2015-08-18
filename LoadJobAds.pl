#!/usr/bin/perl

use strict;
use warnings;

use LWP::UserAgent;
use URI::Escape('uri_escape');
use File::Basename;
use IPC::System::Simple qw(capture capturex);

# directories
my $backup_dir = "/opt/jobAds/";
my $condor_bin_dir = "/home/green22/condor/bin/";

# timestamp
my $timestamp = time();

# files
my $adm_filename = "/home/imaxon/asterix-mgmt/clusters/local/working_dir/condor/job_ads.adm";

# command
my $schedds_command = $condor_bin_dir . "condor_status -schedd -af name";
my $jobads_command = $condor_bin_dir . "condor_history -l -pool cm.chtc.wisc.edu -name %s -cons 'CompletionDate>%s && JobUniverse=!=12'";

# db
my $asterixdb_url = "http://localhost:19002/";
my $asterixdb_dataverse = "use dataverse CondorTest;";
my $count_query = $asterixdb_dataverse . q(
count(for $el in dataset JobAds return $el);
);
my $jobads_insert = $asterixdb_dataverse . q(
insert into dataset JobAds(
	for $el in dataset RawCondorJobAds

	where 

	not(some $check in dataset JobAds satisfies $check.GlobalJobId = $el.GlobalJobId)

	and

	$el.GlobalJobId != ""

	let $RemoteWallClockTimeDuration := duration(string-concat(["P", string($el.RemoteWallClockTime), "S"]))

	return {
		"GlobalJobId": $el.GlobalJobId,
		"Raw" : $el,
		"Owner" : {"name": $el.Owner },
		"ClusterId" : $el.ClusterId,
		"ProcId" : $el.ProcId,
		"RemoteWallClockTime" : $RemoteWallClockTimeDuration,
		"CompletionDate" : datetime-from-unix-time-in-secs($el.CompletionDate),
		"QDate" : datetime-from-unix-time-in-secs($el.QDate),
		"JobCurrentStartDate" : datetime-from-unix-time-in-secs($el.JobCurrentStartDate),
		"JobStartDate" : datetime-from-unix-time-in-secs($el.JobStartDate),
		"JobCurrentStartExecutingDate" : datetime-from-unix-time-in-secs($el.JobCurrentStartExecutingDate),
		"Schedd" : string("%s")
	}
);
);

# list of schedds
my $schedds_command_output = capture($schedds_command) or die "unable to capture $schedds_command: $!\n";
my @schedd_list = split('\n', $schedds_command_output);
my $schedd_list_count = scalar @schedd_list;
my $schedd_count = 0;

my $schedd_backup_dir;
my $jobads_backup_filename;
my $completion_date_filename;
my $completion_date;
my $schedd_jobads_command; 
my $schedd_jobads_output;   
my $formatted_jobads_insert;

# create backup directory 
make_dir($backup_dir);

foreach my $schedd (@schedd_list){

	print_log(($schedd_count++) . "/$schedd_list_count schedds completed");

	# trim white space
	$schedd =~ s/^\s+|\s+$//g;

	# skip if schedd name is empty string
	if (! length $schedd){
		print_log("skipping schedd '$schedd'");
		next;
	}

	# make backup directory for schedd
	$schedd_backup_dir = $backup_dir . $schedd . "/";
	make_dir($schedd_backup_dir);
	
	# backup filename for schedd
	$jobads_backup_filename =  $schedd_backup_dir . "jobAds_" . $timestamp;

		
	$completion_date_filename = $schedd_backup_dir . "NewestCompletionDate";

	if (! -f $completion_date_filename){
		print_log("creating $completion_date_filename");
		get_newest_completion_date($schedd_backup_dir . "jobAds_*");
	}

	open(FILE, "<", $completion_date_filename) or die "unable to open $completion_date_filename\n";
	$completion_date = <FILE>;
	close FILE;

	print_log("completion date $completion_date to generate backup");

	# write raw job ads to file
	print_log("backup raw job ads to $jobads_backup_filename");
	open(FILE, '>', $jobads_backup_filename) or die "Unable to open file '$jobads_backup_filename' $!\n";
	
	$schedd_jobads_command = sprintf($jobads_command,  $schedd, $completion_date);
	
	eval {
		$schedd_jobads_output = capture($schedd_jobads_command); 
	};

	if ($@){
		print_log("Command did not successfully complete '$schedd_jobads_command' $!");
		print_log("skipping schedd '$schedd'");
		next;
	} 

	print FILE $schedd_jobads_output;
	close FILE;

	print_log("check for completion date in $jobads_backup_filename");
	get_newest_completion_date($jobads_backup_filename);

	
	######## remove this
	next;
	########
	
	# convert job ads to ADM
	classad_to_adm($jobads_backup_filename, $adm_filename);
	

	# get count before update
	#print_log("get before count");
	#query($count_query) =~ /(\d+)/;
	#my $before_count =  $1;

	# add schedd to asterixdb command
	$formatted_jobads_insert = sprintf($jobads_insert, $schedd);

	# move raw data into JobAds dataset
	print_log("move data from external dataset to JobAds");
	update($formatted_jobads_insert);

	# get count after update
	#print_log("get after count");
	#query($count_query) =~ /(\d+)/;

	# print count difference
	#print_log(($1 - $before_count) . " JobAds added"); 
}

exit 0;

sub query
{
	my $query = $_[0];
	return asterixdb("query?query=", $query);	
}

sub update
{
	my $statements = $_[0];
	return asterixdb("update?statements=", $statements);
}

# subroutine to run a query on the Asterix DB
sub asterixdb 
{
	my $response;
	my $command = $_[0];
	my $arg = uri_escape $_[1];
	my $url = $asterixdb_url . $command . $arg;
	my $ua = LWP::UserAgent->new();
	my $repeat = 5;	
	my $repeat_wait = 30;

	$ua->timeout(600);
	
	for (my $i = 0; $i < $repeat; $i++){
		$response = $ua->get($url);
		if (! $response->is_success) {
        		print_log("unable to get $url");
        		print_log("status line: " . $response->status_line); 
        		print_log("decoded content: " . $response->decoded_content);
			print_log("wait $repeat_wait secs");
			sleep $repeat_wait;
			print_log("repeat " . ($i + 1));
			next;
		}
		last;
	}
	return $response->decoded_content;
}

# subroutine to check if directory exists
sub make_dir
{	
	my $dir = $_[0];
	if (! -d $dir){
		print_log("make directory $dir");
		mkdir $dir or die "Unable to make directory '$dir' $!";
	}
}

# print to stdout a log like string
sub print_log
{
	print localtime() . " - " . $_[0] . "\n";
}

# create comletion date from scanning job ads
sub get_newest_completion_date
{
	my @filenames = <$_[0]>;

	my $completion_date_filename = dirname($_[0]) . "/NewestCompletionDate";
	my $completion_date = 0;
	if (-f $completion_date_filename)
	{
        	open(FILE, "<", $completion_date_filename)or die "unable to open $completion_date_filename: $!";
        	$completion_date = <FILE>;
        	close(FILE);
	}


	foreach my $filename (@filenames)
	{
        	open(FILE, "<", "$filename") or die "unable to open $filename: $!";
        	my @lines = <FILE>;
        	close(FILE);
        	my $content = join('', @lines);
        	my @matches = $content =~ /CompletionDate\s*=\s*(\d+)/g;

	        push @matches, $completion_date;

        	@matches = sort @matches;

        	$completion_date = $matches[-1];

	}

	open(FILE, ">", $completion_date_filename) or die "unable to open $completion_date_filename: $!";
	print FILE ($completion_date - 10);
	close FILE;

}		

# sub to convert classad to adm.  Code from gthain. 
sub classad_to_adm {

	print_log("convert raw job ad to adm format");

	my $key;
	my $value;
	my $adm_output = "{";
	my $classads_filename = $_[0];
	my $adm_filename = $_[1];

	open(FILE, "<", $classads_filename) or die "unable to open $classads_filename: $!\n";
	while (<FILE>) {

		if(/^$/) {
			$adm_output = substr($adm_output, 0, -1);
			$adm_output .= "\n}{";
			next;
		}

		if (/(.*)\s=\s(.*)\n/){
			$key = $1;
			$value = $2;

			$adm_output .= "\n\"$1\":";

			if($key eq "RemoteWallClockTime"){
                	        $value =~ /(\d+)/;
                       		$adm_output .= "$1,";
                        	next;
                	}

                	if ($value =~ /^["]/) {
                        	$adm_output .= "$value,";
                        	next;
                	}

                	if ($value =~ /^[0-9.e+\-]+$/) {
                        	$adm_output .= "$value,";
                        	next;
                	}

                	if ($value =~ /^(true|false)$/i) {
                        	$adm_output .= "$value,";
                        	next;
                	}

                	$value =~ s/"/\\"/g;

                	$adm_output .= "\"$value\"";
		}

	}	
	$adm_output = substr($adm_output, 0, -1);
	close(FILE);

	open(FILE, '>', $adm_filename);
	print FILE $adm_output;
	close(FILE);
}
