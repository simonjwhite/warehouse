#!perl

use strict;
use warnings;

use Bio::EnsEMBL::DBSQL::DBAdaptor;

print STDERR "Connectng to db...";
my $refdb = new Bio::EnsEMBL::DBSQL::DBAdaptor(
  '-host'   => 'localhost',
  '-user'   => 'root',
  '-dbname' => 'homo_sapiens_core_75_37',
  );
  print STDERR "...done\nFetching Genes...";
  
  my $ga = $refdb->get_GeneAdaptor();
  my @genes = @{$ga->fetch_all('protein_coding')};
  print STDERR "..done. ";
  print " Found " . scalar (@genes) ."\n";
  
  
  
  