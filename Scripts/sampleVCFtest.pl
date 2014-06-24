#!perl
use strict;
use warnings;
use Adaptors::Hbase;
use Modules::SampleVCF;
use Getopt::Long;
use Env qw(REGISTRY);

$| = 1;

my $analysis ="nosqltest";
my $sample;
my $sampleHash;
my $chr;
my $start;
my $end;
my $registry = $REGISTRY;



&GetOptions(
	    'sample:s' => \$sample,
	    'registry:s' => \$registry,
	    'analysis:s' => \$analysis
	    	   );
	
my $usage = "sliceVCFtest.pl 
-sample file of sample names - one per line
";	   
die $usage unless $sample ;

# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							        -port => "9090" ,						        
							        -registry => $registry);

# make the object
my $svcf = Modules::SampleVCF->new(-con     => $db,
								  -sample   => $sample,
								  -analysis => $analysis,
								  -buffer => 1000);
							  
#now make me a vcf
$svcf->vcfFromSample($sample);