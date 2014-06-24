#!perl
use strict;
use warnings;
use Data::Dumper;
use Adaptors::VariantSummaryTable;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Modules::SliceVCF;
use Getopt::Long;
use Env qw(REGISTRY);
$| = 1;

my $sampleHash;
my $tran;
my $type     = 'object';
my $registry = $REGISTRY;
my $analysis = 'chargeSummary';
my $sample   = 'CHARGE';
my $regex    = ".*";
my $vt;
my $exons = 1;

&GetOptions(
			 'registry:s' => \$registry,
			 'tran:s'     => \$tran,
			 'exons:s'	  => \$exons,
			 'regex:s'    => \$regex,
);
my $usage = "sliceVCFtest.pl 
-tran 	 transcript id
";
die $usage unless $tran;
my $reg = 'Bio::EnsEMBL::Registry';
$reg->load_all($registry);
my $transcript_adaptor = $reg->get_adaptor( 'human', 'core', 'transcript' );
my $transcript = $transcript_adaptor->fetch_by_stable_id($tran);
print STDERR "Got transcript $tran here : $transcript\n";

# lets see if we can get a connection
my $db = Adaptors::Connection->new(
									-host     => "hadoop-headnode1",
									-port     => "9090",
									-registry => $registry
);

# make the object
my $va = Adaptors::VariantSummaryTable->new(
											 -con      => $db,
											 -analysis => $analysis,
											 -buffer   => 1000,
											 -sample   => $sample,
											 -type     => 'hash',
											 -versions => 4,
);
$va->namespace("CHARGE_SUMMARY");
my $data = $va->fetchByTranscript( $transcript, $exons );

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

#exit;
