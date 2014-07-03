#!perl
use strict;
use warnings;
use Data::Dumper;
use Adaptors::Hbase;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Modules::SliceVCF;
use Adaptors::VariantSummaryTable;
use Getopt::Long;
use Env qw(REGISTRY);
$| = 1;

my $sample;
my $sampleFile;
my @sampleList;
my $chr;
my $start;
my $end;
my $table;
my $registry = $REGISTRY;
my $analysis ;
my $phenotype;
my $regex =".+";

&GetOptions(
	    'sample:s' => \$sample,
	    'sampleFile:s' => \$sampleFile,    
	    'phenotype:s' => \$phenotype,
	    'table:s'	=> \$table,
	    'analysis:s'=> \$analysis,
	    'regex:s'	=> \$regex
	    	   );
	
my $usage = "sliceVCFtest.pl 
-phenotype 	 name of phenotype to search
-samples file of sample names - one per line
-table	 table to query
";	   
die $usage unless $phenotype && $analysis && $table && $sample;
	   
# load the data
if ( $sampleFile ) {
open (SAM,$sampleFile) or die ("Cannot open sample file $sampleFile\n");
while (<SAM>){
	chomp;
	push @sampleList,$_;
}
}

# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							   -port => "9090",
							   -registry => $registry );

#my $reg = 'Bio::EnsEMBL::Registry';
# $reg->load_all($registry);
  
# get the variant table and send it a phenotype
my $va = Adaptors::VariantTable->new(
											 -con      => $db,
											 -analysis => $analysis,
											 -sample   => $sample,
											 -buffer   => 1000,
											 -type     => 'hash',
											 -versions => 4,
											 -table   => $table
);


my $data = $va->fetchByPhenotype( $phenotype );

# 1st do the header
my $h = 1;
foreach my $thing (@$data) {
	foreach my $var_feat (@$thing) {
		if ($h) {
			print "CHR\tPOS\tREF\tALT\t";
			foreach my $key ( sort keys %$var_feat ) {
				next if ( $key eq 'cs' or $key eq 'chr' or $key eq 'ref' or $key eq 'alt' or $key eq 'end' or $key eq 'start');
				#print "$key\t";
				if ( lc($key)  =~ /$regex/ ) {
					print "$key\t";
				}
			}
			print "\n";
		}
		print $var_feat->{'chr'} . "\t";
		print $var_feat->{'start'} . "\t";	
		print $var_feat->{'ref'} . "\t";
		print $var_feat->{'alt'} . "\t";			
		foreach my $key (sort  keys %$var_feat ) {
			next if ( $key eq 'cs' or $key eq 'chr' or $key eq 'ref' or $key eq 'alt' or $key eq 'end' or $key eq 'start');
				#print "$key\t";
			# then print the values
			if ( lc($key) =~ /$regex/ ) {			
				print $var_feat->{$key} . "\t";
			}
		}
	print "\n";
	$h = 0;
	}

}
