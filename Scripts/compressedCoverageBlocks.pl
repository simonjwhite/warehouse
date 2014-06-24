#!perl
use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use Adaptors::Hbase;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use Bio::EnsEMBL::Registry;

use Modules::VariantCoverage;
use Env qw(REGISTRY);
$| = 1;
my $samtools = "/hgsc_software/samtools/latest/samtools";
my $bam;
my $bin         = 1;
my $sf          = 1;
my $threshold   = 1;
my $ana         = 'nosqltest';
my $table       = 'coverageTest';
my $host        = "hadoop-headnode1";
my $port        = "9090";
my $assembly    = "GRCh37";
my $coordsystem = "c";
my $sam;
my $registry = $REGISTRY;
my $verbose;
my $halt   = 0;
my $region = "";
my $usage  = "compressedCoverageBlocks.pl 
-samtools	path to samtools
-bam		bam file to compute coverage for
-db			Hbase db to connect to 
-port		Habse port
-table		Table to write results to
-analysis   Analysis logic name to store keys with
-sample		Sample name
-bin		Bin size
-scalar		Scale factor
-threshold  Minimum depth before scaling factor is applied
-verbose    Be noisy
-halt		STOP!!!!! once you get to this base
-region		limit samtools to a region
";
my $count = 0;
&GetOptions(
			 'samtools:s'  => \$samtools,
			 'bam:s'       => \$bam,
			 'bin:s'       => \$bin,
			 'sf:s'        => \$sf,
			 'registry:s'  => \$registry,
			 'sample:s'    => \$sam,
			 'table:s'     => \$table,
			 'threshold:s' => \$threshold,
			 'verbose!'    => \$verbose,
			 'halt:s'      => \$halt,
			 'region:s'    => \$region
);
die($usage) unless $samtools && $bam && $sam;

my $reg   = 'Bio::EnsEMBL::Registry';
$reg->load_all($registry);

# need to use sample and analysis ids rather than names
# get analysis id

my $ia = $reg->get_adaptor( "human", "variation", "individual" );
print STDERR "Bins Sorted\n";


# open the stream
my $cmd =
  "$samtools  view -b -F 1796 -q 1  $bam $region | $samtools depth /dev/stdin";
print STDERR "Running $cmd\n";
open( my $fh, "$cmd|" )
  or die("Cannot open stream for command $samtools depth $bam\n");
open( UNC, ">unccompressed.txt" )
  or die("Cannot open uncompressed for writing stream to.\n");
open( DEC, ">deccompressed.txt" )
  or die("Cannot open decompressed for writing data to.\n");
# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							        -port => "9090" ,						        
							        -registry => $registry);
# make the adaptor
my $svcf = Modules::VariantCoverage->new(
								   -con           => $db,
								   -analysis      => $ana,
								   -table         => $table,
								   -type	      => 'hash',
								   -sample		  => $sam,
								   -bin			  => $bin,
								   -scale         => $sf,
								   -threshold     => $threshold,
								   -coordsystem   => 'c',
								   -assembly       => 'GRCh37'
								   );
								    
$svcf->storeStream($fh);