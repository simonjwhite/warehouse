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
use Env qw(REGISTRY);

$| = 1;

my $samtools = "/hgsc_software/samtools/latest/samtools";
my $bam;
my $low = 6;
my $high = 1024;
my $ana = 'nosqltest';
my $table = 'coverageTest';
my $host = "hadoop-headnode1";
my $port = "9090";
my $assembly = "GRCh37";
my $coordsystem = "c";
my $sam;
my $registry = $REGISTRY;

my $usage = "goodCoverageBlocks.pl 
-samtools	path to samtools
-bam		bam file to compute coverage for
-db			Hbase db to connect to 
-port		Habse port
-table		Table to write results to
-analysis   Analysis logic name to store keys with
-sample		Sample name
-low  		Depth min cutoff for calling variants
-high		Max depth cutoff for calling variants
";
my $count = 0;

&GetOptions(
	    'samtools:s' => \$samtools,
	    'bam:s'		=> 	\$bam,
	    'low:s'		=>	\$low,
	    'high:s'	=>	\$high,
	    'registry:s' => \$registry,
	    'sample:s'	=>	\$sam,
	    'table:s'	=>	\$table,
	    	   );
die($usage) unless $samtools && $bam && $sam;
my $start = 0;
my $end =0;
my $chr= 0;
my $total = 0;
my $time = time;
# open connection

# lets see if we can get a connection
my $socket = Thrift::Socket->new( $host, $port );
$socket->setSendTimeout(100000);    # 100 seconds (value is in millisec)
$socket->setRecvTimeout(200000);    # 200 seconds (value is in millisec)
my $transport = Thrift::BufferedTransport->new($socket);
my $protocol  = Thrift::BinaryProtocol->new($transport);
my $client    = Hbase::HbaseClient->new($protocol);
eval { $transport->open(); };

if ($@) {
	print "Unable to connect: $@->{message}\n";
	exit 1;
}
my $reg = 'Bio::EnsEMBL::Registry';
 $reg->load_all($registry);
# need to use sample and analysis ids rather than names
# get analysis id
my $aa =  $reg->get_adaptor( "human", "core", "analysis" );
my $analysis = $aa->fetch_by_logic_name($ana);
print STDERR "Got analysis " . $analysis->logic_name . "\n";

my $ia =  $reg->get_adaptor( "human", "variation", "individual" );
my $sample = $ia->fetch_all_by_name($sam)->[0];



my $type = 'low';
# open the stream
open (STREAM, "$samtools  view -b -F 1796 -q 1  $bam | $samtools depth /dev/stdin |") or die("Cannot open stream for command $samtools depth $bam\n"); 
while (<STREAM>){
	chomp;
	#print "$_\n";
	#last if $count > 1000;
	my @line = split("\t");
	
	# no data
	if ($line[2] >= $low && $line[2] <= $high){
		$start = $line[1] unless $start;
		$end = $line[1];
	} else {
		if ($start && $end ){
			my $s = sprintf "%09d", $start;
			my $e = sprintf "%09d", $end;
			my $rk = $coordsystem.":".$assembly.":".$line[0] .":".$s.":".$e.":1_".$analysis->dbID."_" .$sample->dbID;
			#print "$rk\n";
			my $kvs->{'D'} = "1";
			store($rk,$kvs);
			# reset for the next one
			$start = 0;
			$end = 0;
			$count++;
		}
	}
	if ( $chr ne $line[0]){
		# last row
		if( $start ){
			my $s = sprintf "%09d", $start;
			my $e = sprintf "%09d", $line[1];
			my $rk = $coordsystem.":".$assembly.":".$chr .":".$s.":".$e.":1_".$analysis->dbID."_" .$sample->dbID;
			my $kvs->{'D'} = "1";
			store($rk,$kvs);
			$count++;
		}
		$start = 0;
		$end = 0;
	}
	$chr = $line[0];

	if ( $count >=  1000){
		$total += $count;
		my $s = time - $time;
		$s += 1 ;# stop division by zero 
		my $num = sprintf("%2d", $total / $s);
		print STDERR "Stored $total blocks in $s seconds ( $num blocks per second )\r";
		$count = 0;
	}
}

if ( $start && $end ){
	my $s = sprintf "%09d", $start;
	my $e = sprintf "%09d", $end;
	my $rk = $coordsystem.":".$assembly.":".$chr .":".$s.":".$e.":1_".$analysis->dbID."_" .$sample->dbID;
	my $kvs->{'D'} = "Covered";
	store($rk,$kvs);
}

print STDERR "\nFinished - " . ($total + $count) ." blocks in " . (time - $time). " seconds\n";

sub store {
	my ($row,$kvs) = @_;
	my $mutations;
	eval {
		foreach my $key ( keys %$kvs) {
			push @{$mutations},Hbase::Mutation->new ( { column => $key.":", value => $kvs->{$key}});
			#print "Attempting to store $row ,$key, ". $kvs->{$key} ."\n";
		}
		$client->mutateRow ( $table, $row, $mutations );	

	}; if ($@){
		print STDERR "Error storing to Hbase:\n";
		print Dumper $@;
		exit;
	}
}