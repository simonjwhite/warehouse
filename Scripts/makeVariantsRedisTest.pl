#!perl

use strict;
use warnings;


use Bio::EnsEMBL::Variation::Variation;

use Hypertable::Thriftclient;
use Getopt::Long;

my $redis = Redis->new;
my $reg;

my $usage = "makeVariantsRedisTest.pl
-wildcard 	what should I search the keys for?
";
my $file;


GetOptions(
  'wildcard=s'      => \$reg,
  );
  die ($usage) unless $reg;

# fetch the things?
# checking how many we have
# dont use module for fetching it does not bother streaming - even for a simple count

# stream the fucker
my $stream = redisKeyStream($reg);
my @buffer;
my $lastPos = "";
while (<$stream>){
	chomp;
	#print "\nKEY $_	\n";
	# lets build a freaking object then
	#split the keys
	my ($position,$analysis,$sample) = split"_";
	#print "$position $analysis $sample \t";
	my @keys = $redis->hkeys($_);
	my %hash;
	foreach my $key ( @keys ){
		#print "KEY $key \n";
		$hash{$key} = $redis->hget($_, $key);
	} 
	$lastPos = $position;
	print "$position $analysis $sample:\n";
	foreach my $key (keys %hash){
		print "\t $key -> " . $hash{$key} ."\n";
	}
	print "\n";
}




sub redisKeyStream {
	my ($reg) = @_;
	my $cmd = "/Users/simonw/Local/redis-2.8.8/src/redis-cli keys $reg";
	my $fh;
	# stream the fucker
	open ($fh,"$cmd |") or die ("Pipe is buggered\n");
	return $fh;
}
	
