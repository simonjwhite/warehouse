#!perl


use strict;
use warnings;
use Data::Dumper;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use Hbase::Hbase;




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

print "column families in vcf:\n";
my $descriptors = $client->getColumnDescriptors ("vcf");
foreach my $col (sort keys %{$descriptors})
{
	printf ("  column: {%s}, maxVer: {%s}\n", $descriptors->{$col}->{name}, $descriptors->{$col}->{maxVersions} );
}
my $time = time;
# test 1
for ( my $i = 1 ; $i<=10000 ; $i++){
my $row = "chromosome:CGRh37:1:123456:123456$i:1|analysis|sample";
my $format = "GT:VR:RR:DP:GQ";

my $info = "ReqIncl=.;PU=.;RFG=synonymous_SNV;GI=ISG15:NM_005101:exon2:c.A294G:p.V98V";

my $mutations = [ Hbase::Mutation->new ( { column => "ID:", value => "." }),
					Hbase::Mutation->new ( { column => "REF:", value => "A" }),
					Hbase::Mutation->new (  { column => "ALT:", value => "G" }),
					Hbase::Mutation->new (  { column => "QUAL:", value => "23" }),
					Hbase::Mutation->new (  { column => "FILTER:", value => "PASS" }),
					Hbase::Mutation->new (  { column => "INFO:", value => $info }),
					Hbase::Mutation->new (  { column => "FORMAT:", value => $format }),
					Hbase::Mutation->new (  { column => "SAMPLE:", value => "BOOLSHIT" }) ];

					
	$client->mutateRow ( "vcf", $row, $mutations )
};		
$transport->close();
print "test 1 took " .(time - $time) ." seconds\n";
$time = time;	 
#test2			 
for ( my $i = 1 ; $i<=10000 ; $i++){
	$transport->open();
my $row = "chromosome:CGRh37:1:123456:123456$i:2|analysis|sample";
my $format = "GT:VR:RR:DP:GQ";

my $info = "ReqIncl=.;PU=.;RFG=synonymous_SNV;GI=ISG15:NM_005101:exon2:c.A294G:p.V98V";

my $mutations = [ Hbase::Mutation->new ( { column => "ID:", value => "." }),
					Hbase::Mutation->new ( { column => "REF:", value => "A" }),
					Hbase::Mutation->new (  { column => "ALT:", value => "G" }),
					Hbase::Mutation->new (  { column => "QUAL:", value => "23" }),
					Hbase::Mutation->new (  { column => "FILTER:", value => "PASS" }),
					Hbase::Mutation->new (  { column => "INFO:", value => $info }),
					Hbase::Mutation->new (  { column => "FORMAT:", value => $format }),
					Hbase::Mutation->new (  { column => "SAMPLE:", value => "BOOLSHIT" }) ];

					
	$client->mutateRow ( "vcf", $row, $mutations );
	$transport->close();	
};		
			 
print "test 2 took " .(time - $time) ." seconds\n";	 			 