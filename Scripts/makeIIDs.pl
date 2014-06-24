#!perl

use strict;
use warnings;
use Env qw(REGISTRY);
use Bio::EnsEMBL::Registry;
use Getopt::Long;
$| = 1;


my $registry = $REGISTRY;
my $sliceSize = 1000000;
my $db = 'core';
my $species = 'human';
my $help;

&GetOptions(
	    'size:s'  	=> \$sliceSize,
	    'registry:s'=> \$registry,
	    'db:s'		=> \$db,
	    'species:s' => \$species,
	    'h!'		=> \$help
	    );

	
my $usage = "makeIIDs.pl 
	    'size:s'  	=> sliceSize - 1000000 by default,
	    'registry:s'=> registry - default is $REGISTRY,
	    'db:s'		=> core by default,
	    'species:s' => human by default,
	    'h!'		=> help me
";	   
die $usage unless $registry || !$help;
	   
my $reg = 'Bio::EnsEMBL::Registry';
$reg->load_all($registry);

my $sa = $reg->get_adaptor($species,$db,"slice");

# fetch them all
my $slices = $sa->fetch_all('toplevel');
foreach my $slice (@$slices){
	my ($cs,$ass,$chr,$start,$end,$strand) = split(":",$slice->name);
	for (my $i = 1 ; $i <= $end; $i+= $sliceSize ){
		print "$cs:$ass:$chr:$i:".($i+$sliceSize -1 ).":$strand\n" if $i+$sliceSize -1 < $end;
		print "$cs:$ass:$chr:$i:$end:$strand\n" if $i+$sliceSize -1 >= $end;
	} 
}