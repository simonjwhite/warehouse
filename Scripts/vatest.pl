#!perl
use strict;
use warnings;
use Data::Dumper;
use Adaptors::Hbase;
use Bio::EnsEMBL::DBSQL::DBAdaptor;

# lets see if we can get a connection
my $db = Adaptors::Hbase->new( -host => "hadoop-headnode1",
							   -port => "9090" );
my $core = Bio::EnsEMBL::DBSQL::DBAdaptor->new(
									 -user   => 'simonw',
									 -host   => 'sug-esxa-db1',
									 -pass   => 'simonw',
									 -dbname => 'simonw_human_37_RNASeq_refined'
);

my $type = shift;
$type = 'text' unless $type;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
my $sa = $core->get_SliceAdaptor();
my $ta = $core->get_TranscriptAdaptor();
my $slice = $sa->fetch_by_region('toplevel',1,200002000,200002200);

print "Test 1 fetching by slice " . $slice->name ."\n";

my $va = $db->getVariantAdaptor($type);
my $list = ["REF","ALT","FORMAT"];

my $data =  $va->fetchBySlice($slice);
	print Dumper $data;

my $transcript = $ta->fetch_by_stable_id("ENST00000544455");

 print "test 2 fetching exons of ENST00000544455\n";
$va->columnList($list);
#$data  = $va->fetchByTranscript($transcript,1);

#print Dumper $data;

# now limit to some samples
my $hash ;
$hash->{"B00PVACXX-8-ID06"}=1;
$hash->{"B00U6ACXX-7-ID07"}=1;
$hash->{"B00U6ACXX-3-ID10"}=1;
$va->sampleHash($hash);
$data  = $va->fetchByTranscript($transcript,1);
print Dumper $data;
