#!/usr/bin/perl

use LWP::UserAgent;
use URI::Escape('uri_escape');

use strict;
use warnings;

my $asterixdb_url = "http://stress4.chtc.wisc.edu:19002/";
my $asterixdb_datavers = "use dataverse CondorTest";

my $raw_job_ads_insert = $asterixdb_dataverse . q(
drop dataset RawCondorJobAds if exists;
create external dataset RawCondorJobAds(RawCondorJobType) 
using localfs(("path"="localhost://condor/job_ads.adm"),("format"="adm"));
); 

my $create_job_ads = $asterix_datavers . q(
create type OwnerType as open{
    name:string
};

create type JobAdsType as open {
	GlobalJobId: string,
	Raw: RawCondorJobType,
	Owner: OwnerType,
	ClusterId: int32,
	ProcId: int32,
	RemoteWallClockTime: duration,
	CompletionDate: datetime,
	QDate : datetime,
	JobCurrentStartDate: datetime,
	JobStartDate : datetime,
	JobCurrentStartExecutingDate : datetime
};

create dataset JobAds(JobAdsType) primary key GlobalJobId;

);

my $ua = LWP::UserAgent->new();
my $ddl_url = $asterixdb_url . "ddl?ddl=". uri_escape $create_job_ads;
my $response = $ua->get($ddl_url);
if (! $response->is_success) {
        die "Unable to get $ddl_url\n" .
        $response->status_line . "\n" .
        $response->decoded_content . "\n";
}

