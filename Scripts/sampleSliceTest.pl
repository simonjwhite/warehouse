#!perl
use strict;
use warnings;
use Data::Dumper;
use Adaptors::Hbase;
use Bio::EnsEMBL::Variation::VariationFeatureOverlap;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Bio::EnsEMBL::Variation::TranscriptVariation;

use Modules::SliceVCF;
use Getopt::Long;
use Env qw(REGISTRY);
$| = 1;

my $sample;
my $sampleHash;
my $tran;
my $type = 'object';
my $registry = $REGISTRY;
my $analysis ;
my $vt ;
my $table;

&GetOptions(
	    'sample:s'  => \$sample,
	    'analysis:s'=> \$analysis,
	    'registry:s'=> \$registry,
	    'tran:s'	=> \$tran,
	    'type:s'	=> \$type,
	   	'var_type:s'=> \$vt,
	   	'table:s'	=> \$table,
	    	   );
	
my $usage = "sliceVCFtest.pl 
-tran 	 transcript id
-sample  sample name
-analysis analysis name
-table 	table
";	   
die $usage unless $sample && $tran && $table && $analysis;
	   

my $reg = 'Bio::EnsEMBL::Registry';
$reg->load_all($registry);

my $transcript_adaptor = $reg->get_adaptor('human', 'core', 'transcript');
my $tva = $reg->get_adaptor('human', 'webVariation', 'transcriptVariation');
print "TVA $tva\n";
my $transcript = $transcript_adaptor->fetch_by_stable_id($tran); 


# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							        -port => "9090" ,						        
							        -registry => $registry);

# make the object
my $va = Adaptors::VariantTable->new(-con   => $db,
								  -sample   => $sample,
								  -analysis => $analysis,
								  -buffer 	=> 1000,
								  -type  	=> $type,
								  -varType  => $vt,
								  -table    => $table);
			


	my $data = $va->fetchBySampleTranscript($sample,$tran,1,1) ;


#exit;


foreach my $thing ( @$data){
	foreach my $var_feat (@$thing){
	print "$var_feat\n";
	my $tv;
	eval {
   $tv = Bio::EnsEMBL::Variation::TranscriptVariation->new(
        -transcript        => $transcript,
        -variation_feature => $var_feat,
        -adaptor           => $tva
    );
	}; if ($@){
		print STDERR "WTF? $@\n";
	}
	print "\nVariant " . $var_feat->feature_Slice->name ."\n";
    print "consequence type: ", (join ",", @{$tv->consequence_type}), "\n";
    print "cdna coords: ", $tv->cdna_start, '-', $tv->cdna_end, "\n";
    print "cds coords: ", $tv->cds_start, '-', $tv->cds_end, "\n";
    print "pep coords: ", $tv->translation_start, '-',$tv->translation_end, "\n";
    print "amino acid change: ", $tv->pep_allele_string, "\n";
    print "codon change: ", $tv->codons, "\n";
    print "allele sequences: ", (join ",", map { $_->variation_feature_seq } 
        @{ $tv->get_all_TranscriptVariationAlleles }), "\n";	
	}
}