#!perl

use strict;
use warnings;
use Data::Dumper;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use Hbase::Hbase;



sub printRow($)
{
	my $rowresult = shift;

	return if (!$rowresult || @{$rowresult} < 1);
	# rowresult is presummed to be a Hbase::TRowResult object

	printf ("row: {%s}, cols: \n", $rowresult->[0]->{row});
	my $values = $rowresult->[0]->{columns};
	foreach my $key ( sort ( keys %{$values} ) )
	{
		printf ("{%s} => {%s}\n", $key, $values->{$key}->{value});
	}
}

my $host = $ARGV[0] || "hadoop-headnode1";
my $port = $ARGV[1] || 9090;

my $socket = Thrift::Socket->new ($host, $port);
$socket->setSendTimeout (10000);		# Ten seconds (value is in millisec)
$socket->setRecvTimeout (20000);		# Twenty seconds (value is in millisec)

my $transport = Thrift::BufferedTransport->new ($socket);
my $protocol = Thrift::BinaryProtocol->new ($transport);
my $client = Hbase::HbaseClient->new ($protocol);

eval {
	$transport->open ();
};
if ($@)
{
	print "Unable to connect: $@->{message}\n";
	exit 1;
}

my $demo_table = "demo_table";

print "scanning tables...\n";

#
# Search for all the tables in the HBase DB, return value is an arrayref
#
my $tables = $client->getTableNames();
foreach my $table (sort @{$tables})
{
	print "  found {$table}\n";
	# This client will re-create the $demo_table, so we need to drop it first
	if ($table eq $demo_table)
	{
		# Before we can drop a table, it has to be disabled first
		if ($client->isTableEnabled ($table))
		{
			print "    disabling table: {$table}\n";
			$client->disableTable ($table);
		}
		# We assume the table has been disabled at this point
		print "    deleting table: {$table}\n";
		$client->deleteTable ($table);
	}
}

#
# Create the demo table with two column families, entry: and unused:
#
my $columns = [
	Hbase::ColumnDescriptor->new ( { name => "entry:", maxVersions => 10 } ),
	Hbase::ColumnDescriptor->new ( { name => "unused:" } ),
	];

print "creating table: {$demo_table}\n";
eval {
	# This can throw Hbase::IllegalArgument (HASH)
	$client->createTable ( $demo_table, $columns );
};
if ($@)
{
	die "ERROR: Unable to create table {$demo_table}: $@->{message}\n";
}

print "column families in {$demo_table}:\n";
my $descriptors = $client->getColumnDescriptors ($demo_table);
foreach my $col (sort keys %{$descriptors})
{
	printf ("  column: {%s}, maxVer: {%s}\n", $descriptors->{$col}->{name}, $descriptors->{$col}->{maxVersions} );
}

##
## Test UTF-8 handling
##
my $invalid = "foo-\xfc\xa1\xa1\xa1\xa1\xa1";
#my $valid = "foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB";
#
## non-utf8 is fine for data
my $key = "foo";
my $mutations = [ Hbase::Mutation->new ( { column => "entry:$key", value => $invalid } ) ];
##$client->mutateRow ( $demo_table, $key, $mutations );
#
## try emptry strings
#$key = "";
#$mutations = [ Hbase::Mutation->new ( { column => "entry:$key", value => "" } ) ];
#$client->mutateRow ( $demo_table, $key, $mutations );
#
### this row name is valid utf8
#$key = "foo";
## $mutations = [ Hbase::Mutation->new ( column => "entry:$key", value => $valid ) ];
## This is another way to use the Mutation class
#my $mutation = Hbase::Mutation->new ();
#$mutation->{column} = "entry:$key";
#$mutation->{value} = $valid;
#$mutations = [ $mutation ];
#$client->mutateRow ( $demo_table, $key, $mutations );

# non-utf8 is not allowed in row names
#eval {
#	$mutations = [ Hbase::Mutation->new ( column => "entry:$key", value => $invalid ) ];
#	# this can throw a TApplicationException (HASH) error
#	$client->mutateRow ($demo_table, $key, $mutations);
#	die ("shouldn't get here!");
#};
#if ($@)
#{
#	print "expected error: $@->{message}\n";
#}

#
# Run a scanner on the rows we just created
#
print "Starting scanner...\n";
$key = "";
# scannerOpen expects ( table, key, <column descriptors> )
# if key is empty, it searches for all entries in the table
# if column descriptors is empty, it searches for all column descriptors within the table
my $scanner;

eval {
	print "0 $key\n";
 $scanner = $client->scannerOpen ( $demo_table, $key, [ "entry:" ] );
	# scannerGet returns an empty arrayref (instead of an undef) to indicate no results
	print "A";
	my $result = $client->scannerGet ( $scanner );
	print "B";
	while ( $result && @{$result} > 0 )
	{
		print "C";
		printRow ( $result );
		print "D";
		$result = $client->scannerGet ( $scanner );
	}
	print "F";
	$client->scannerClose ( $scanner );
	print "Scanner finished\n";
};
if ($@)
{
	print Dumper $@;
	$client->scannerClose ( $scanner );
	print "Scanner finished\n";
}
#
# Run some operations on a bunch of rows
#
for (my $e = 100; $e > 0; $e--)
{
	# format row keys as "00000" to "00100";
	my $row = sprintf ("%05d", $e);

	$mutations = [ Hbase::Mutation->new ( { column => "unused:", value => "DELETE_ME" } ) ];
	$client->mutateRow ( $demo_table, $row, $mutations );
	printRow ( $client->getRow ( $demo_table, $row ) );
	$client->deleteAllRow ( $demo_table, $row );

	$mutations = [
		Hbase::Mutation->new ( { column => "entry:num", value => "0" } ),
		Hbase::Mutation->new ( { column => "entry:foo", value => "FOO" } ),
		];	
	$client->mutateRow ( $demo_table, $row, $mutations );
	printRow ( $client->getRow ( $demo_table, $row ) );

	$mutations = [
		Hbase::Mutation->new ( { column => "entry:foo", isDelete => 1 } ),
		Hbase::Mutation->new ( { column => "entry:num", value => -1 } ),
		];	
	$client->mutateRow ( $demo_table, $row, $mutations );
	printRow ( $client->getRow ( $demo_table, $row ) );

	$mutations = [
		Hbase::Mutation->new ( { column => "entry:num", value => $e } ),
		Hbase::Mutation->new ( { column => "entry:sqr", value => $e * $e } ),
		];
	$client->mutateRow ( $demo_table, $row, $mutations );
	printRow ( $client->getRow ( $demo_table, $row ) );

	$mutations = [ 
		Hbase::Mutation->new ( { column => "entry:num", value => -999 } ),
		Hbase::Mutation->new ( { column => "entry:sqr", isDelete => 1 } ),
		];

	# mutateRowTs => modify the row entry at the specified timestamp (ts)
	$client->mutateRowTs ( $demo_table, $row, $mutations, 1 ); # shouldn't override latest
	printRow ( $client->getRow ( $demo_table, $row ) );

	my $versions = $client->getVer ( $demo_table, $row, "entry:num", 10 );
	printf ( "row: {%s}, values: \n", $row );
	foreach my $v ( @{$versions} )
	{
		printf ( "  {%s} @ {%s}\n", $v->{value}, $v->{timestamp} );
	}

	eval {

		my $result = $client->get ( $demo_table, $row, "entry:foo" );

		# Unfortunately, the API returns an empty arrayref instead of undef
		# to signify a "not found", which makes it slightly inconvenient.
		die "shouldn't get here!" if ($result && @{$result} > 0);

		if (!$result || ($result && @{$result} < 1))
		{
			print "expected: {$row} not found in {$demo_table}\n";
		}
	};
	if ($@)
	{
		print "expected error: $@\n";
	}
}

my $column_descriptor = $client->getColumnDescriptors ( $demo_table );
$columns = [];
foreach my $col ( keys %{$column_descriptor} )
{
	my $colname = $column_descriptor->{$col}->{name};
	print "column with name: {$colname}\n";
	push ( @{$columns}, $colname);
}

print "Starting scanner...\n";
$scanner = $client->scannerOpenWithStop ( $demo_table, "00020", "00040", $columns );
eval {

	# scannerGet returns an empty arrayref (instead of an undef) to indicate no results
	my $result = $client->scannerGet ( $scanner );
	while ( $result && @$result > 0 )
	{
		printRow ( $result );
		$result = $client->scannerGet ( $scanner );
	}

	$client->scannerClose ( $scanner );
	print "Scanner finished\n";
};
if ($@)
{
	$client->scannerClose ( $scanner );
	print "Scanner finished\n";
}

$transport->close ();

exit 0;

