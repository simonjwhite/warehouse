#!perl
use strict;
use warnings;
use Adaptors::Hbase;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Modules::SliceVCF;
use Getopt::Long;
use Env qw(REGISTRY);
$| = 1;

my $samples;
my @sampleList;
my $chr;
my $start;
my $end;
my $table;
my $cov;
my $registry = $REGISTRY;
my $analysis = 'nosqltest';
my $ct;
my $vt;
my $common;

&GetOptions(
	    'samples:s' => \$samples,
	    'chr:s'		=> \$chr,
	    'start:s'	=> \$start,
	    'end:s'		=> \$end,
	    'table:s'	=> \$table,
	    'coverage:s'  => \$cov,
	   	'ct:s'  	=> \$ct,
	    'analysis:s'=> \$analysis,
	    'vartype:s'=> \$vt,
	    'common!'	=> \$common
	    	   );
	
my $usage = "sliceVCFtest.pl 
-chr 	  chromsome
-start 	  start pos	
-end 	  end pos
-samples  file of sample names - one per line
-table	  table to query
-coverage use coverage yes / no
-ct 	  table with coverage data to query default VARIANT_COVERAGE
-vartype  SNP / Indel (optional)
-common   Just output a bed file showing the count of vars at each site
";	   
die $usage unless $samples && $chr && $start && $end;
	   
# load the data
open (SAM,$samples) or die ("Cannot open sample file $samples\n");
while (<SAM>){
	chomp;
	push @sampleList,$_;
}

# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							   -port => "9090",
							   -registry => $registry );
my $core = Bio::EnsEMBL::DBSQL::DBAdaptor->new(
									 -user   => 'simonw',
									 -host   => 'sug-esxa-db1',
									 -pass   => 'simonw',
									 -dbname => 'simonw_human_37_RNASeq_refined'
);

my $sa = $core->get_SliceAdaptor();
my $slice = $sa->fetch_by_region('toplevel',$chr,$start,$end);

# make the object
my $svcf = Modules::SliceVCF->new(-con      => $db,
								  -analysis => $analysis,
								  -table    => $table,
								  -coverage => $cov,
								  -coverageTable => $ct,
								  -varType  => $vt
								  );
# give it the list of samples								  
$svcf->sampleList(\@sampleList);
						
if ( $common ){
	$svcf->varCountFromSlice($slice);
}	else {	  
	#now make me a vcf
	$svcf->vcfFromSlice($slice);
}