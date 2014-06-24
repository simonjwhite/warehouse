#!perl


=head1 CONTACT

  Please email comments or questions to simow@bcm.edu

=cut

=head1 USAGE: 
cat <TSV file to import> | perl import_TSV_to_Hbase.pl 

-analysis	Analysis name 
-table 		Table name
-sample		Sample name /  project name

Required:
1st line is column headers.
Headers must contain chrom, start or pos, and optionally end

=cut

use strict;
use warnings;
use Adaptors::Hbase;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Modules::SliceVCF;
use Data::Dumper;
use Getopt::Long;
use Bio::EnsEMBL::Registry;
use Env qw(REGISTRY);
$| = 1;

my $sample_name;
my $table;
my $registry = $REGISTRY;
my $logic_name;
my $head = 1;

&GetOptions(
	    'sample:s'  => \$sample_name,
	    'table:s'	=> \$table,
	    'analysis:s'=> \$logic_name,
	    'head:s'    => \$head
	    );
die usage() unless $sample_name && $logic_name  && $table;
	   


# lets see if we can get a connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							   -port => "9090",
							   -registry => $registry );
my $client = $db->client;
my $reg = 'Bio::EnsEMBL::Registry';
$reg->load_all($registry);

# get all the associated stuff before writing to the cluster
# pre-fetching the slice names
my $sa =   $reg->get_adaptor( 'human', "core", "slice" );
# cache seq region names
my %seq_names;
foreach my $slice ( @{ $sa->fetch_all('toplevel') } ) {
	my $name = $slice->name;
	#print "NAME $name\n";
	my @tmp = split( ":", $name );
	# use abbrev for seqnames
	my $cs = substr( $tmp[0], 0, 1 );
	$seq_names{ $tmp[2] } = $cs . ":" . $tmp[1] . ":" . $tmp[2] . ":";
} 
die("ERROR: seq_region not populated\n")
unless scalar keys %seq_names;

# get analysis id
my $aa = $reg->get_adaptor( 'human', "core", "analysis" );
my $analysis = $aa->fetch_by_logic_name($logic_name);
print STDERR "Got analysis " . $analysis->logic_name . "\n";

# fetch/store the sample
my $sample = getSample($sample_name);

# stream in the input file
my $fh = \*STDIN;
my @header;
my $time = time;
my $totalTime;
my $headerLine;
my $cnt  = 0 ;
my $total = 0;
my $pass = 0;
my $chrC;
my $posC;
my $endC;
my $rowKey;
my $lastRowKey;
my $string;
while (<$fh>){
	chomp;
	$cnt++;
	my @line = split("\t");
	# parse and store the header
	if ( $cnt == 1){
		for (my $i = 0 ; $i < scalar(@line) ; $i++){
			# check we have the required components
			my $cell = lc($line[$i]);
			if ($cell eq 'chrom'){
				$chrC = $i;
			} elsif ($cell eq 'pos' or $cell eq 'start'){
				$posC = $i;
			} elsif ($cell eq 'end'){
				$endC = $i;
			} else {
				# write out the line to store
				$headerLine.=$cell."\t";
				# store the order of the cells for later
				push @header,$i;
			}  
		}
		die("Cannot parse chromosome and position out of the header using chrom and pos/start:\n$_\n")unless defined($chrC) && defined($posC);
		# store the header
		my $kvs->{'D'} = $headerLine;
		htableStore( $sample->dbID . "_" . $analysis->dbID . "_HEADER", $kvs );
		next;
	}
	# get the rowkey
	my $chr = $line[$chrC];
	my $pos = $line[$posC];
	# 0 padding 
	$pos = sprintf "%09d", $pos;
	my $end ="";
	$end = sprintf "%09d", $end if $end;
	my $row;
	foreach my $column (@header){
		$row.= $line[$column]."\t";
	}
	# trim off the last tab
	$row =~ s/\t$//;
	
	$end = $line[$endC].":" if $endC;
	$rowKey = "c:GRCh37:$chr:$pos$end:";

	# store the row(s)
	$total++;
	my $string;
	# remove trailing 0s if there are any
	foreach my $cell ( split("\t",$row)){
		if ($cell =~/^0(\d+)/){
			$string .= "$1\t";
		} else {
			$string .= "$cell\t";
		}
	}
	my $kvs->{'D'} = $string;
	htableStore( $rowKey .$sample->dbID . "_" . $analysis->dbID , $kvs );		
	$lastRowKey = $rowKey;

	if ($cnt > 5000){ 
		$time = sprintf("%.1f",time - $time);
		$time = 1 if $time == 0;
		$totalTime += $time;
		#print "TIME  $time " . time ."\n"; 
		my $rate = sprintf("%.2f",$total / $totalTime);
		print STDERR "Stored $total rows in $totalTime seconds $rate rows/s \r" ;
		$cnt = 2;
		$time = time;
	} 
}


exit 0;

sub getSample {
	my ( $name ) = @_;
	# fetch the adaptor
	my $ia = $reg->get_adaptor( 'human', "variation", "individual" );
	die("Failed to fetch individual adaptor for  $name\n") unless $ia;
	my $inds = $ia->fetch_all_by_name($name);
	my $ind;
	if ( scalar @$inds > 1 ) {
			die("ERROR: Multiple individuals with name $name found, cannot continue\n");
		} elsif ( scalar @$inds == 1 ) {
			$ind = $inds->[0];
		}
		# create new
		else {
			$ind = Bio::EnsEMBL::Variation::Individual->new(
												  -name    => $name,
												  -adaptor => $ia,
												  -type_individual => 'outbred',
												  -display => 'UNDISPLAYABLE',
			);
			$ia->store($ind);
		}
	return $ind;
}	

sub htableStore {
	my ( $row, $kvs ) = @_;
	my $mutations;
	eval {
		foreach my $key ( keys %$kvs )
		{
			push @{$mutations}, Hbase::Mutation->new(
							  { column => $key . ":", value => $kvs->{$key} } );
			#print "Attempting to store $row ,$key, ". $kvs->{$key} ."\n";
		}
		$client->mutateRow( $table, $row, $mutations );
	};
	if ($@) {
		print STDERR "Error storing to Hbase:\n";
		print Dumper $@;
		exit;
	}
}


sub usage {
	exec( 'perldoc',$0);
	exit;
}