#!perl

use strict;
use warnings;

use Bio::EnsEMBL::Registry;
use Redis;

my $redis = Redis->new;


my %seq_names;
my %analyis_ids;
my %sample_ids;
my $anlaysis = "NOSQLTEST";
my $registry = "/Users/simonw/git/ensembl-variation/scripts/import/ensembl.registry";
my $species = "human";
my $person = "C-CHS-51860";
my $reg = 'Bio::EnsEMBL::Registry';

$reg->load_all($registry);


my $refdb = $reg->get_DBAdaptor($species, 'core') 
    or die "Failed to get core DBAdaptor";

my $vardb = $reg->get_DBAdaptor($species, 'variation') 
    or die "Failed to get variation DBAdaptor";
  
  
  print STDERR "...done\nFetching Slices...";

	my $sa = $refdb->get_SliceAdaptor;


	# cache seq region names
  foreach my $slice ( @{$sa->fetch_all('toplevel')}){
  	my $name = $slice->name;
  	#print "NAME $name\n";
  	my @tmp = split(":",$name);
  	$seq_names{$tmp[2]} = $tmp[0].":".$tmp[1].":".$tmp[2].":";
  }
  print STDERR "...done.\n";
  # get the analysis
  my $aa = $refdb->get_AnalysisAdaptor;
  my $a = $aa->fetch_by_logic_name('NOSQLTEST');
  print STDERR "Analysis " . $a->logic_name ." - ID: " .$a->dbID ."\n";
  
  # load the variation db and get the sample data
  # this is going to be the tricky one right?
  print STDERR "Fetching individuals from variation database...";
  my $ia = $vardb->get_IndividualAdaptor();

  my @folk = @{$ia->fetch_all()};
  print STDERR "...done\n";
  foreach my $person (@folk){
  	$sample_ids{$person->name} =  $person->dbID;
  }

  my $sample_id = $sample_ids{$person};
  my $analysis_id = $a->dbID;
  # load a vcf and generate a list of seq_regions keys
  
  print STDERR "..done\nLoading vcf\n";
  
  open(VCF ,"/Users/simonw/TestData/SNP2.vcf") or die ("Cannot open small.vcf\n");
  my $count = 0;
  my $time = time;
  while(<VCF>){
  	$count++;

  	chomp;
  	#print "$_\n";
  	next if $_ =~ /^#/;
  	my @line = split("\t");
  	my $chr = $line[0];
  	# generate the key
  	my $name = $seq_names{$line[0]};
  	my $key = "SEQID = " . $name. $line[1].":".$line[1].":1=$sample_id=$analysis_id";
  	my $value = "null";
  	$redis->set($key => $value);
  	
  }
  print STDERR "...done\n";
  my $done = time - $time;
print "Generated $count keys in $done seconds\n";  
