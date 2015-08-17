#!/usr/bin/perl

use strict; 
use warnings;

use File::Basename;

my @filenames = @ARGV;


my $completion_date_filename = dirname($filenames[0]) . "/NewestCompletionDate";
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

exit 0;
