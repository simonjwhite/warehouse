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
my $registry = $REGISTRY;
my $analysis = 'nosqltest';

&GetOptions(
	    'samples:s' => \$samples,
	    'chr:s'		=> \$chr,
	    'start:s'	=> \$start,
	    'end:s'		=> \$end,
	    'table:s'	=> \$table,
	    'analysis:s'=> \$analysis
	    	   );
	
my $usage = "sliceVCFtest.pl 
-chr 	 chromsome
-start 	 start pos	
-end 	 end pos
-samples file of sample names - one per line
-table	 table to query
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
								  -table	=> $table,
								  -analysis => $analysis);
# give it the list of samples								  
$svcf->sampleList(\@sampleList);
								  
print "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";
foreach my $sample ( sort @sampleList){
	print "\t$sample";
} 
print "\n";
#now make me a vcf
$svcf->vcfFromSlice($slice);