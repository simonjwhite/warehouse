#!perl

use strict;
use warnings;
use Data::Dumper;
use Hypertable::ThriftClient;
my $client;
eval{
$client = new Hypertable::ThriftClient("hadoop-headnode1", 8000);

print "client $client\n";
my $namespace = $client->namespace_open("script_PopulateDB");
print Dumper($client->hql_exec($namespace,"show tables"));
#print Dumper($client->hql_exec($namespace,"select * from VCFcontents max_versions 1"))

print "scanner examples\n";
my $scanner = $client->scanner_open($namespace, "VCFcontents",
    new Hypertable::ThriftGen::ScanSpec({versions => 1}));

my $cells = $client->scanner_get_cells($scanner);

while (scalar @$cells) {
  print "$cells\n";
  foreach my $cell ( @$cells ) {
    #print "$cell\n";
    print $cell->{'key'}->row ." ". $cell->{'key'}->column_family ." " . $cell->{'value'} ."\n";

  }
  last;
  $cells = $client->scanner_get_cells($scanner);
}


print "REGEX test\n";
$scanner = $client->scanner_open($namespace, "VCFcontents",
    new Hypertable::ThriftGen::ScanSpec({versions => 1, value_regexp=>"G", columns=>["ALT"]}));

my $cells = $client->scanner_get_cells($scanner);

while (scalar @$cells) {
  foreach my $cell ( @$cells ) {
   print $cell->{'key'}->row ." ". $cell->{'key'}->column_family ." " . $cell->{'value'} ."\n";
   }
  $cells = $client->scanner_get_cells($scanner);
}
print "mutator examples\n";
my $mutator = $client->mutator_open($namespace, "VCFcontents");
my $key = new Hypertable::ThriftGen::Key({row => 'perl-k1',
                                          column_family => 'FORMAT'});
my $cell = new Hypertable::ThriftGen::Cell({key => $key,
                                            value => 'this is a test'});
$client->mutator_set_cell($mutator, $cell);
$client->mutator_flush($mutator);
$client->mutator_close($mutator);

print "REGEX test2\n";
$scanner = $client->scanner_open($namespace, "VCFcontents",
    new Hypertable::ThriftGen::ScanSpec({versions => 1, row_regexp=>"^perl*"}));

my $cells = $client->scanner_get_cells($scanner);

while (scalar @$cells) {
  foreach my $cell ( @$cells ) {
   print $cell->{'key'}->row ." ". $cell->{'key'}->column_family ." " . $cell->{'value'} ."\n";
   }
  $cells = $client->scanner_get_cells($scanner);
}
}; if ( $@){
	print "$@\n";
}