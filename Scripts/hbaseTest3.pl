#!perl
use strict;
use warnings;
use Data::Dumper;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use Hbase::Hbase;
my $host = "hadoop-headnode1";
my $port= 9090;
my $socket = Thrift::Socket->new( $host, $port );
$socket->setSendTimeout(10000);    # Ten seconds (value is in millisec)
$socket->setRecvTimeout(20000);    # Twenty seconds (value is in millisec)
my $transport = Thrift::BufferedTransport->new($socket);
my $protocol  = Thrift::BinaryProtocol->new($transport);
my $client    = Hbase::HbaseClient->new($protocol);
eval { $transport->open(); };

if ($@) {
	print "Unable to connect: $@->{message}\n";
	exit 1;
}
my $start = shift;

#   my $scanner = $client->scannerOpen('chargeVCFtest',new Hbase::Hbase_scannerOpenWithPrefix_args(
#  {
#  	startAndPrefix => "chromosome:GRCh37:10:100020572",
#  }),[]);
my $scanner = $client->scannerOpenWithPrefix( 'chargeVCFtest', $start, [] );

# scannerGet() never returns undef, only an empty array ref [] to
# signifiy "no more data"
my $result = $client->scannerGet($scanner);
my $row = "";
if ( $result && @{$result} > 0 ) {

	# get the 1st row
	$row = $result->[0]->{'row'};
	print "1st row = $row .... carrying on from there \n";
} else {
	$row = $start;
}
     # start an openended scan from this position
my $fwd = $client->scannerOpen( 'chargeVCFtest', $row, [] );
$result = $client->scannerGet($fwd);
my $count = 0;
while ( $fwd && @{$result} > 0 ) {
	$count++;

	# $result is an array ref of Hbase::TRowResult
	#print Dumper $result;
	my $var = $result->[0];
	print $var->{'row'} . "\t:\n";

	#	foreach my $col ( keys %{$var->{'columns'}} ){
	#
	#		print "$col = " .  $var->{'columns'}->{$col}->{'value'} ."\n";
	#	}
	# 	last if $var->{'row'} !~ /^chromosome:GRCh37:10:100003785/;
	$result = $client->scannerGet($fwd);
}
