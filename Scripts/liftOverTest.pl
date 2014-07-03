#!perl
use strict;
use warnings;
use Adaptors::Hbase;
use Modules::SampleVCF;
use Getopt::Long;
use Env qw(REGISTRY);
use Bio::EnsEMBL::ApiVersion;


$| = 1;

my $analysis ="nosqltest";
my $sample;
my $sampleHash;
my $chr;
my $start;
my $end;
my $registry = $REGISTRY;
my $cs;
my $table = 'ADSP'; 
my $annotation;


&GetOptions(
	    'sample:s' => \$sample,
	    'registry:s' => \$registry,
	    'analysis:s' => \$analysis,
	    'liftover:s' => \$cs,
	    'table:s'	=>	\$table,
	    'annotation:s' => \$annotation
	    	   );
	
my $usage = "liftOverTest.pl 
-sample  	name
-liftover 	coordsystem to lift over to
-annotation csv of tables to query
";	   
die $usage unless $sample ;

my @aa = split(",",$annotation);

# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							        -port => "9090" ,						        
							        -registry => $registry);

# make the object
my $svcf = Modules::SampleVCF->new(-con     => $db,
								  -sample   => $sample,
								  -analysis => $analysis,
								  -liftover => $cs,
								  -table    => $table,
								  -buffer => 10000,
								  -annotation => \@aa);
							  
#now make me a vcf
$svcf->vcfFromSample($sample);