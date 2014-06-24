#!perl
# Prototype hbase connection by Simon White
package Adaptors::Hbase;
use strict;
use Bio::EnsEMBL::Utils::Exception qw(throw);
use Bio::EnsEMBL::Utils::Argument qw(rearrange);
use Data::Dumper;
use Adaptors::VariantTable;
use Adaptors::Connection;
my $cursor = "";

sub new {
	my ( $class, @args ) = @_;
	my $self = bless {}, $class;
	my ( $type, $con, $buffer, $analysis, $sample, $versions, $ass, $liftover )
	  = rearrange(
				   [
					 'TYPE',   'CON',      'BUFFER',   'ANALYSIS',
					 'SAMPLE', 'VERSIONS', 'ASSEMBLY', 'LIFTOVER'
				   ],
				   @args
	  );
	if ( defined $con ) {
		$self->dbc($con);
	} else {
		$self->dbc( Adaptors::Connection->new(@args) );
	}

	# defaults
	$type   = 'text' unless $type;
	$buffer = 500000 unless $buffer;
	$self->type($type);
	$self->buffer($buffer);
	if ($versions) {
		$self->versions($versions);
	} else {
		$self->versions(1);
	}
	$self->sample( $self->getSample($sample) )       if $sample;
	$self->analysis( $self->getAnalysis($analysis) ) if $analysis;
	$self->liftover($liftover)                       if $liftover;

	# store in the hash as well
	$self->analysisList( [$analysis] ) if $analysis;
	$self->assembly($ass);
	return $self;
}

# generates the sample hash from a list of sample names
sub sampleList {
	my ( $self, $list ) = @_;
	my %hash;
	$self->throw("sampleList requires a list of sample names\n")
	  unless $list
	  and ref($list) eq "ARRAY";
	foreach my $sam (@$list) {

		# fetch from the db
		my $sam = $self->getSample($sam);
		$hash{ $sam->dbID } = $sam;
	}
	$self->sampleHash( \%hash );
	return;
}

# generates the analysis hash from a list of analysis names
sub analysisList {
	my ( $self, $list ) = @_;
	my %hash;
	$self->throw("analysisList requires a list of analysis names\n")
	  unless $list
	  and ref($list) eq "ARRAY";
	foreach my $ana (@$list) {

		# fetch from the db
		my $ana = $self->getAnalysis($ana);
		$hash{ $ana->dbID } = 1;
	}
	$self->analHash( \%hash );
	return;
}

sub getAnalysis {
	my ( $self, $name ) = @_;
	my $aa       = $self->dbc->analysisAdaptor;
	my $analysis = $aa->fetch_by_logic_name($name);
	$self->throw("Cannot find analysis $name using $aa \n")
	  unless $analysis;
	return $analysis;
}

sub getSample {
	my ( $self, $name ) = @_;
	my $va     = $self->dbc->sampleAdaptor;
	my $sample = $va->fetch_all_by_name($name);
	$self->throw("Cannot find sample $name using $va \n")
	  unless $sample && ref($sample) eq "ARRAY" && scalar(@$sample) > 0;
	return $sample->[0];
}

sub fetchAllBySample {
	my ( $self, $sample ) = @_;
	$self->keyList( $self->getSampleKeyList($sample) );

	# start at the begining of the list
	$self->listCursor( $self->keyList->[0] );
	return $self;
}

sub getSampleKeyList {
	my ( $self, $sample_name ) = @_;

	# only store it once
	if ( my $list = $self->sampleKeyList($sample_name) ) {
		return $list;
	}
	my $sample = $self->getSample($sample_name);
	my $str;
	my @keyList;
	my $batch = 0;
	do {
		$batch++;
		my $row =
		  $self->analysis->dbID . ":" . $sample->dbID . ":LOOKUP:$batch";
		my $data = $self->tableOperation( 'getRow', $row );
		$str = $data->[0]->{'columns'}->{'D:'}->{'value'};
		if ($str) {
			push( @keyList, split( "_", $str ) );
		}
		print STDERR "Got "
		  . scalar(@keyList)
		  . " keys from the database for batch $batch from sample "
		  . $self->sample->name . "\r";
	} while ($str);
	print "\n";
	$self->sampleKeyList( $sample->name, \@keyList );
	return \@keyList;
}

sub getHeader {
	my ( $self, $sample, $analysis ) = @_;
	$analysis = $self->analysis unless $analysis;
	my $sample = $self->getSample($sample);
	my $row    = $self->analysis->dbID . ":" . $self->sample->dbID . ":HEADER";
	my $data   = $self->tableOperation( 'getRow', $row );
	my $header = $data->[0]->{'columns'}->{'D:'}->{'value'};
	return $header;
}

sub getMeta {
	my ($self)   = @_;
	my $analysis = $self->analysis;
	my $sample   = $self->sample;
	my $row      = $self->analysis->dbID . ":" . $self->sample->dbID . ":META";
	my $data = $self->tableOperation( 'getRow', $row );
	my $meta = $data->[0]->{'columns'}->{'D:'}->{'value'};
	return $meta;
}

sub fetchBySampleSlice {
	my ( $self, $sample_name, $slice, $startPos, $analysis ) = @_;
	$analysis = $self->analysis unless $analysis;
	my @newList;
	print STDERR "StartPos $startPos\n";
	$startPos = 0 unless $startPos;
	my $i;
	print STDERR "Starting from $startPos\n";

	# get the key list and then intersect by slice
	my $keyList = $self->getSampleKeyList($sample_name);

	# now just get the bits that lie on the slice
	for ( $i = $startPos ; $i < scalar(@$keyList) ; $i++ ) {
		my $key = $keyList->[$i];
		my ( $cs, $asm, $chr, $start, $end ) = split( ":", $key );
		next unless $slice->seq_region_name eq $chr;

		# use end position if supplied
		$end = $start unless $end;
		next unless $end >= $slice->start;
		last if $start >= $slice->end;
		print STDERR "Pushing key $key\n";
		push @newList, $key;
	}
	print STDERR "got " . scalar(@newList) . " keys \n";
	if ( scalar(@newList) > 0 ) {
		$self->keyList( \@newList );

		# just fetch those keys
		$self->listCursor( $self->keyList->[0] );
		return ( $self, $i );
	}
	return ( undef, $i );
}

sub fetchAllByAnalysis {
}

sub fetchAllByPopulation {
}

sub fetchBySlice {
	my ( $self, $slice ) = @_;

	# generate the slice prefix
	$self->throw(   "fetchBySlice requires a Bio::EnsEMBL::Slice object not a "
				  . $slice->isa
				  . "\n" )
	  unless $slice->isa("Bio::EnsEMBL::Slice");
	my ( $cs, $ass, $chr, $start, $end, $strand ) = split( ":", $slice->name );

	# fetch the starting row key
	my $row = substr( $cs, 0, 1 );
	$row .= ":$ass:$chr:";
	$row .= sprintf "%09d", $start;
	$row .= ":";

	#set the cursor to this position
	$self->sliceCursor($row);
	$self->end($end);
	return $self;
}

sub storeData {
	my ( $self, $cs, $chr, $start, $end, $value ) = @_;

	# basic storage method
	# test that the data must is complient with the allowed format
	#  cs : assembly : chr :start : end: analysis id : sample_id
	# checks:
	$self->throw("Cannot recognise coordinate system $cs\n")
	  unless $cs;
	$self->throw("Cannot recognise chromosome $chr\n")
	  unless $chr;
	$self->throw("Cannot recognise position start $start\n")
	  unless $start && $start =~ /\d+/;
	$self->throw("Cannot recognise position end $end\n")
	  if $end && $end !~ /\d+/;
	$self->throw("No value specified\n")
	  unless ($value);
	$self->throw(   "Cannot store without an analysis "
				  . $self->analysis
				  . " and a sample "
				  . $self->sample
				  . " \n" )
	  unless $self->analysis && $self->sample;

	# 0 pad start and end
	$start = sprintf( "%09d", $start );
	$end   = sprintf( "%09d", $end ) if $end;

	# make a lookup key and a full key
	my $lookup = "$cs:" . $self->assembly . ":$chr:$start:$end";
	my $key    = "$lookup:" . $self->analysis->dbID . ":" . $self->sample->dbID;

	#	print "$key\t$value\n";
	# add a separator to the lookup
	$lookup .= "_";
	$self->_store( $key, $value );
	return $lookup;
}

sub _store {
	my ( $self, $key, $value ) = @_;

	# private method to write to the db
	my $mutations;
	eval {
		push @{$mutations},
		  Hbase::Mutation->new( { column => 'D:', value => $value } );

		#print STDERR "Storing " . $self->namespace, " $key, $mutations\n";
		$self->tableOperation( 'mutateRow', $key, $mutations );
	};
	if ($@) {
		$self->throw("Error storing to Hbase:\n$@\n");
	}
	return;
}

sub storeMeta {
	my ( $self, $meta ) = @_;

	# store meta data
	my $key = $self->analysis->dbID . ":" . $self->sample->dbID . ":META";

	#print "$key\t$meta\n";
	$self->_store( $key, $meta );
	return;
}

sub storeMetaStart {
	my ( $self, $meta ) = @_;
	$self->storeMeta(   "Storing Sample="
					  . $self->sample->name
					  . " Analysis="
					  . $self->analysis->logic_name
					  . " starting at "
					  . localtime );
	return;
}

sub storeMetaFinish {
	my ( $self, $meta ) = @_;
	$self->storeMeta(   "Storing Sample="
					  . $self->sample->name
					  . " Analysis="
					  . $self->analysis->logic_name
					  . " finished at "
					  . localtime );
	return;
}

sub rangeStore {
	my ( $self, $chr, $end, $analysis, $sample, $data ) = @_;
	my $key = "$analysis:$sample";
	if ($key) {
		$self->{'_rangeStore'}->{$key}->{'end'}  = $end;
		$self->{'_rangeStore'}->{$key}->{'chr'}  = $chr;
		$self->{'_rangeStore'}->{$key}->{'data'} = $data;
	}
	return;
}

# return a list of active keys to check for ranges
sub rangeKeys {
	my ($self) = @_;
	my @output;
	foreach my $key ( keys %{ $self->{'_rangeStore'} } ) {
		push @output, $key;
	}
	return \@output;
}

sub rangeQuery {
	my ( $self, $analysis, $sample, $chr, $position ) = @_;
	my $key = "$analysis:$sample";
	if ($key) {

		# check the chromosome
		if ( $chr eq $self->{'_rangeStore'}->{$key}->{'chr'} ) {

			# check the position
			if ( $position <= $self->{'_rangeStore'}->{$key}->{'end'} ) {

				# return the data
				return $self->{'_rangeStore'}->{$key}->{'data'};
			}
		}

# if we have failed one or more of these tests we assume we have passed the end of the range
		print STDERR
		  "Destroying hash $key at position $chr : $position\nHASH : "
		  . $self->{'_rangeStore'}->{$key}->{'chr'} . " : "
		  . $self->{'_rangeStore'}->{$key}->{'end'} . "\n";
		delete $self->{'_rangeStore'}->{$key};
	}
	return;
}

sub storeHeader {
	my ( $self, $header ) = @_;

	# store meta data
	my $key = $self->analysis->dbID . ":" . $self->sample->dbID . ":HEADER";

	#print "$key\t$header\n";
	$self->_store( $key, $header );
	return;
}

sub storeLookup {
	my ( $self, $lookup, $batch ) = @_;
	$self->throw("Need batch information to store lookup\n")
	  unless $batch && $batch =~ /\d+/;

	# store meta data
	my $key =
	  $self->analysis->dbID . ":" . $self->sample->dbID . ":LOOKUP:$batch";

	#print "$key\t$lookup\n";
	$self->_store( $key, $lookup );
	return;
}

sub bufferedSliceFetch {
	my ($self) = @_;
	my $output;

	# starting position
	my $row = $self->sliceCursor;
	print STDERR "Cursor = $row \n";
	return 0 unless $row;

	# which columns to fetch
	my $sliceEnd = $self->end;
	print STDERR "END $sliceEnd\n";
	my $count    = 0;
	my $last_row = 0;
	my $last_chr = 0;
	print STDERR "Opening scanner on table " . $self->namespace . "\n";
	my $scanner = $self->tableOperation( 'scannerOpen', $row, [] );
	my $result = $self->tableOperation( 'scannerGet', $scanner );
	my $sampleHash = $self->sampleHash();
	my $analHash   = $self->analHash();
  RESULT: while ( $result && @{$result} > 0 ) {
		my $var = $result->[0];
		my $rk  = $var->{'row'};

		# fetch multiple entries if needed
		if ( $self->versions > 1 ) {

		  #print STDERR 'Fetching ' . $self->versions ." versions. using $rk\n";
			$result =
			  $self->tableOperation( 'getVer', $rk, "D", $self->versions );
		}
		foreach $var (@$result) {
			if ( $self->versions > 1 ) {

				# getVer returns HbaseTcell objects
				# rather than HbaseTrowResult
				# need to alter the hash to make it compatible
				$var->{'columns'}->{'D:'}->{'value'} = $var->{'value'};
				$var->{'row'} = $rk;
			}

			#$rk = $var->{'row'};
			# check the keys are consistant
			$self->throw("Key not recognised $rk \n")
			  unless $rk =~ /^\w+:\S+:\S+:\d+:\d*:.*$/;

			# parse the position and meta data out of the row key
			my ( $cs, $ass, $chr, $start, $stop, $analysis, $sample ) =
			  split( ":", $rk );

			# check for range keys
			if ($stop) {

				# store the range
				$self->rangeStore( $chr, $stop, $analysis, $sample, $var );
			}

			# test for end of slice or fall off end onto next chr
			if ( $start > $sliceEnd || ( $last_chr && $chr ne $last_chr ) ) {

				# finished reset the cursor and end
				print STDERR "Finished\n";
				$self->sliceCursor(0);
				$self->end(0);
				last RESULT;
			}

			# filter on sample
			if ($sampleHash) {
				next unless $sampleHash->{$sample};
			}

			# filter on analysis
			if ($analHash) {
				next unless $analHash->{$analysis};
			}
			$count++;

			# check buffer size when we move onto a new line
			if (    $last_row
				 && $count > $self->buffer
				 && ( $start > $last_row || $chr ne $last_chr ) )
			{

				# set the cursor to the new row and return what we have
				print STDERR "returning buffer at pos $count \n";
				$self->sliceCursor($rk);
				last;
			}

			# if we have moved to a new row - check for
			# any range keys before going any further
			unless ( $last_chr eq $chr && $last_row eq $start ) {
				my $keys = $self->rangeKeys();
				if ($keys) {
					foreach my $rk (@$keys) {

						# add this data into the output too
						my ( $rangeAnalysis, $rangeSample ) = split( ":", $rk );
						my $data =
						  $self->rangeQuery( $rangeAnalysis, $rangeSample, $chr,
											 $start );
						push @{$output}, $self->process($data) if $data;
					}
				}
			}

			# do something with the data
			push @{$output}, $self->process($var);
			$last_chr = $chr;
			$last_row = $start;
		}
		$result = $self->tableOperation( 'scannerGet', $scanner );
	}
	return $output;
}

sub bufferedListFetch {
	my ($self) = @_;
	my $output;

	# get the list
	my $keyList = $self->keyList;

	# starting position
	my $cursor = $self->listCursor;
	print STDERR "Cursor = $cursor \n";
	return 0 unless $cursor;

	# position of the cursor in the list
	my $start;
	for ( my $i = 0 ; $i < scalar(@$keyList) ; $i++ ) {
		if ( $keyList->[$i] eq $cursor ) {
			$start = $i;
		}
	}
	$self->throw("Cannot find start position in key list from cursor $cursor\n")
	  unless defined $start;
	my $sample = $self->sample;
	$self->throw("Cannot do list fetch without sample name\n")
	  unless defined $sample;
	my $end        = $self->end;
	my $count      = 0;
	my $last_row   = 0;
	my $last_chr   = 0;
	my $lc         = 0;
	my $sampleHash = $self->sampleHash();
	my $analHash   = $self->analHash();

	for ( my $i = $start ; $i < scalar(@$keyList) ; $i++ ) {
		my $row =
		    $keyList->[$i] . ":"
		  . $self->analysis->dbID . ":"
		  . $self->sample->dbID;
		my $result = $self->tableOperation( 'getRow', $row );

		# get data
		my $var = $result->[0];
		my $rk  = $var->{'row'};

		# check the keys are consistant
		$self->throw("Key not recognised  $rk fetching using $row\n")
		  unless $rk =~ /^\w+:\S+:\S+:\d+:.*:\d+:\d+$/;

		# parse the position and meta data out of the row key
		my @pos = split( ":", $rk );

		# filter on analysis
		if ($analHash) {
			next unless $analHash->{ $pos[-2] };
		}
		$count++;

		# check buffer size when we move onto a new line
		if (    $last_row
			 && $count > $self->buffer
			 && ( $pos[3] > $last_row || $pos[2] ne $last_chr ) )
		{

			# set the cursor to the new row and return what we have
			print STDERR "returning buffer at pos $count \n";

			# set the cursor
			$lc = $keyList->[$i];
			last;
		}

		# do something with the data
		push @{$output}, $self->process($var);
		$last_chr = $pos[2];
		$last_row = $pos[3];
	}

	# finished reset the cursor
	$self->listCursor($lc);
	return $output;
}

sub nextSlice {
	my ($self) = @_;
	my $data = $self->bufferedSliceFetch();
	return $data;
}

sub nextList {
	my ($self) = @_;
	return $self->bufferedListFetch();
}

sub fetchByPhenotype {
	my ( $self, $phenotype ) = @_;
	my $output;
	my $start = 0;

	# pull out the phenotype regions
	# fetch based on those
	# Get a list of all phenotypes.
	$self->throw("Cannot fetch by phenotype without a phenotype supplied\n")
	  unless $phenotype;
	my $pa         = $self->dbc->phenotypeAdaptor;
	my $pfa        = $self->dbc->phenotypeFeatureAdaptor;
	my $phenotypes = $pa->fetch_all;
	my @list;
	$phenotype = uc($phenotype);
	print STDERR "Looking for $phenotype\n";

	foreach my $p (@$phenotypes) {
		if ( $p->description =~ /$phenotype/ ) {
			print "GOT " . $p->description . " for $phenotype\n";
			push( @list, $p );
		}
	}
	if ( scalar(@list) > 0 ) {
		foreach my $p (@list) {
			print STDERR "Fetching features for " . $p->description . "\n";
			my $pf = $pfa->fetch_all_by_phenotype_id_source_name( $p->dbID );
			foreach my $feat (@$pf) {
				if ( $feat->seq_region_name =~ /^\d+$|^MT$|^X$|^Y$/ ) {

					# do nothing
				} else {
					print STDERR "Ignoring feat on "
					  . $feat->seq_region_name
					  . " only working on reference sequence at the moment\n";
					next;
				}
				if ( $feat->type ne 'Variation' ) {
					print STDERR "Ignoring feat on "
					  . $feat->seq_region_name
					  . " type "
					  . $feat->type
					  . " Only looking at variants at the moment\n";
					next;
				}
				print "GOT $feat "
				  . $feat->seq_region_name . " : "
				  . $feat->start . "\n";
				print "Clinical significance: "
				  . $feat->clinical_significance . "\n";
				print "Associated Gene name " . $feat->associated_gene . "\n";
				my $studies = $feat->associated_studies;
				print "Reference " . $feat->external_reference . "\n";
				foreach my $s (@$studies) {
					print "Study " . $s->description . "\n";
				}
				print "TYPE " . $feat->type . "\n";

				# now lets fetch the regions associated with these:
				my ( $iterator, $s ) =
				  $self->fetchBySlice( $feat->feature_Slice, $start );
				$start = $s;
				print STDERR "Feature "
				  . $feat->seq_region_name . ":"
				  . $feat->start . "-"
				  . $feat->end . "\n";
				if ($iterator) {
					while ( my $o = $iterator->nextSlice ) {
						push @{$output}, $o if $o;
					}
				}
			}
		}
	} else {
		print STDERR "No matches found for $phenotype\n";
	}
	return $output;
}

sub fetchByGene {
	my ( $self, $gene ) = @_;
	$self->throw(   "fetchBySlice requires a Bio::EnsEMBL::Gene object not a "
				  . $gene->isa
				  . "\n" )
	  unless $gene->isa("Bio::EnsEMBL::Gene");
	my $slice = $gene->feature_Slice;
	return $self->fetchBySlice($slice);
}

sub fetchBySampleGene {
	my ( $self, $sample, $gene ) = @_;
	my $sample = $self->getSample($sample);
	$self->throw(   "fetchBySlice requires a Bio::EnsEMBL::Gene object not a "
				  . $gene->isa
				  . "\n" )
	  unless $gene->isa("Bio::EnsEMBL::Gene");
	my $slice = $gene->feature_Slice;
	return $self->fetchBySampleSlice( $sample, $slice );
}

sub fetchByTranscript {
	my ( $self, $transcript, $exons ) = @_;
	my $output;
	$self->throw(
				"fetchBySlice requires a Bio::EnsEMBL::Transcript object not a "
				  . $transcript->isa
				  . "\n" )
	  unless $transcript->isa("Bio::EnsEMBL::Transcript");
	unless ($exons) {
		my ($iterator) = $self->fetchBySlice( $transcript->feature_Slice );
		if ($iterator) {
			while ( my $o = $iterator->nextSlice ) {
				push @{$output}, $o if $o;
			}
		}
		return $output;
	}

	# otherwise just fetch exon regions
	foreach my $e ( sort { $a->start <=> $b->start }
					@{ $transcript->get_all_Exons() } )
	{
		my ($iterator) = $self->fetchBySlice( $e->feature_Slice );
		print STDERR "Exon "
		  . $e->seq_region_name . ":"
		  . $e->start . "-"
		  . $e->end . "\n";
		if ($iterator) {
			while ( my $o = $iterator->nextSlice ) {
				push @{$output}, $o if $o;
			}
		}
	}
	return $output;
}

sub fetchBySampleTranscript {
	my ( $self, $sample_name, $tran, $exons ) = @_;
	my $output;
	my $start = 0;
	my $ta    =
	  $self->dbc->registry->get_adaptor( $self->dbc->species, "core",
										 "transcript" );
	my $transcript = $ta->fetch_by_stable_id($tran);
	$self->throw(   "Cannot find transcript for $tran object not a "
				  . $transcript->isa
				  . "\n" )
	  unless $transcript && $transcript->isa("Bio::EnsEMBL::Transcript");
	unless ($exons) {
		my ($iterator) =
		  $self->fetchBySampleSlice( $sample_name, $transcript->feature_Slice );
		if ($iterator) {
			while ( my $o = $iterator->nextList ) {
				push @{$output}, $o if $o;
			}
		}
	}

	# otherwise just fetch exon regions
	foreach my $e ( sort { $a->start <=> $b->start }
					@{ $transcript->get_all_Exons() } )
	{
		my ( $iterator, $s ) =
		  $self->fetchBySampleSlice( $sample_name, $e->feature_Slice, $start );
		$start = $s;
		print STDERR "Exon "
		  . $e->seq_region_name . ":"
		  . $e->start . "-"
		  . $e->end . "\n";
		if ($iterator) {
			while ( my $o = $iterator->nextList ) {
				push @{$output}, $o if $o;
			}
		}
	}
	return $output;
}

sub tableOperation {
	my ( $self, $op, $value1, $value2, $value3 ) = @_;
	my $client = $self->dbc->client;

# eval the operation
#	print STDERR "Running client->$op('".$self->namespace."','".$value1,$value2,$value3."')\n";
	my $result;
	eval {
		$result = $client->$op( $self->namespace, $value1, $value2, $value3 );
	};
	if ($@) {
		$self->throw(   "Error running $op on table "
					  . $self->namespace
					  . " with value $value1,$value2,$value3\n$@\n" );
	}
	return $result;
}

sub fetchType {
	my ($self) = @_;
}

sub list {
	my ($self) = @_;

	# list the tables in the db
	my $tables = $self->tableOperation('getTableNames');
	return $tables;
}

# move to different coord system
sub liftoverData {
	my ( $self, $data ) = @_;
	my $assembly = $self->liftover;
	my $sa     = $self->SliceAdaptor;
	# check we have it
	my $asma = $self->AssemblyMapperAdaptor();
	my $csa  = $self->CoordSystemAdaptor();

	my $hash = $self->parseRowKey($data);

	# fetch coord system we are on
	my $csf = $csa->fetch_by_name( $hash->{'CS'}, $hash->{'ASS'} );

	# fetch coord system we want to map to
	my $cst = $csa->fetch_by_name( $hash->{'CS'}, $self->liftover );
	my $asm_mapper = $asma->fetch_by_CoordSystems( $csf, $cst );

	# variants rather than ranges
	my $end = $hash->{'END'};
	$end = $hash->{'START'} unless $end;
	my @chr_coords =
	  $asm_mapper->map( $hash->{'CHR'}, $hash->{'START'}, $end, 1, $csf );
	  my $cnt = 0 ;
	foreach my $c (@chr_coords) {
		if ( $c->isa("Bio::EnsEMBL::Mapper::Gap") ) {
			print STDERR "Variant liftover  " . $hash->{"ASS"} .":" .
			$hash->{"CHR"} . ":" . 
			$hash->{"START"} . "-" . 
			$hash->{"END"} ." has a gap : " .
			$self->liftover .":" .$c->start ."-".$c->end."\n";
		}
		if ( $c->isa("Bio::EnsEMBL::Mapper::Coordinate") ) {
			$cnt++;
			my $slice = $sa->fetch_by_seq_region_id($c->id);
		#	print STDERR "COORDS "
		#	. $slice->seq_region_name ." " 
		#	  . $c->start . " "
		#	  . $c->end . "\n";
		# check the ref?
		# modify the row key
		my $row = $data->{'row'};
		#print "ROW $row becomes ";
		my @array = split("\t",$row);
		$array[0] = substr($slice->coord_system->name,0,1);
		$array[1] = $self->liftover;
		$array[2] = $slice->seq_region_name;
		$array[3] = $c->start;
		$array[4] = $c->end;
		#print STDERR  join(":",@array) ."\n";
		$data->{'row'} = join(":",@array) ;
		}
	}
	if ($cnt == 1){
		#checks out
		return $data;
	} else {
		print STDERR "Variant " . $hash->{"ASS"} .":" .
			$hash->{"CHR"} . ":" . 
			$hash->{"START"} . "-" . 
			$hash->{"END"} ." Did not liftover cleanly\n";
		return undef;
	}

}


# do any processing to the data before we format it
sub process {
	my ( $self, $data ) = @_;
	# do we want to add any annotation?
	# do we want to liftover?
	if ( $self->liftover ) {
		$data = $self->liftoverData($data);
		# in case it does not liftover cleanly
		return unless $data;
	}

	# do the formatting
	return $self->format($data);
}

# this deals with the output of the data - text - hash - or ensembl object
# ensembl objects can only be defined by overriding this method in
# inherited classes
sub format {
	my ( $self, $data ) = @_;
	my $format = $self->type;
	my $output;
	if ( $format eq 'object' ) {
		$self->throw("Base class cannot format output as objects");
	}
	if ( $format eq "text" ) {

		# just output as plain text
		my $string = "";
		my $row    = $data->{'row'};
		foreach my $txt ( split( ":", $row ) ) {
			$string .= "$txt\t";
		}
		foreach my $col ( keys %{ $data->{'columns'} } ) {
			$string .= "$col\t" . $data->{'columns'}->{$col}->{'value'} . "\t";
		}
		$string =~ s/\t$//;
		$output = $string;
	}
	if ( $format eq "hash" ) {

		# output as the thrift hash
		$output = $data;
	}
	return $output;
}

# adaptors
sub getVariantAdaptor {
	my ( $self, $type, $buffer, $analysis ) = @_;
	unless ( $self->variantAdaptor ) {
		$analysis = $self->analysis->logic_name unless $analysis;
		$type = 'text' unless $type;

		# make a variant adaptor
		my $va = Adaptors::VariantTable->new(
											  -con      => $self->dbc,
											  -type     => $type,
											  -buffer   => $buffer,
											  -analysis => $analysis
		);
		$self->variantAdaptor($va);
	}
	return $self->variantAdaptor;
}

sub getVariantCoverageAdaptor {
	my ( $self, $type, $buffer, $analysis ) = @_;
	unless ( $self->variantCoverageAdaptor ) {
		$analysis = $self->analysis->logic_name unless $analysis;
		$type = 'text' unless $type;

		# make a variant adaptor
		my $va = Adaptors::VariantCoverageTable->new(
													  -con      => $self->dbc,
													  -type     => $type,
													  -buffer   => $buffer,
													  -analysis => $analysis
		);
		$self->variantCoverageAdaptor($va);
	}
	return $self->variantCoverageAdaptor;
}

sub parseRowKey {
	my ( $self, $hash ) = @_;
	my $data;
	unless ( $hash ){
		$self->throw("What the what ?  $hash \n");
	}
	my @pos = split( ":", $hash->{'row'} );

	# unpack the coord system names
	$pos[0] = 'chromosome'  if $pos[0] eq 'c';
	$pos[0] = 'supercontig' if $pos[0] eq 's';
	$data->{'SAMPLE'} = $pos[-1];
	$data->{'CHR'}    = $pos[2];
	$data->{'START'}  = $pos[3];
	$data->{'END'}    = $pos[4] if $pos[4];
	$data->{'CS'}     = $pos[0];
	$data->{'ASS'}    = $pos[1];
	return $data;
}
########### Containers ##################
sub variantAdaptor {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( $value->isa("Adaptors::VariantTable") ) {
			$self->throw(   "Adaptor type should be Adaptors::VariantTable not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'variantAdaptor'} = $value;
	}
	return $self->{'variantAdaptor'};
}

sub variantCoverageAdaptor {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( $value->isa("Adaptors::VariantCoverageTable") ) {
			$self->throw(
					"Adaptor type should be Adaptors::VariantCoverageTable not "
					  . ref($value)
					  . "\n" );
		}
		$self->{'variantCoverageAdaptor'} = $value;
	}
	return $self->{'variantCoverageAdaptor'};
}

sub sliceAdaptor {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( $value->isa("Bio::EnsEMBL::DBSQL::SliceAdaptor") ) {
			$self->throw(
				   "Adaptor type should be a::EnsEMBL::DBSQL::SliceAdaptor not "
					 . ref($value)
					 . "\n" );
		}
		$self->{'sliceAdaptor'} = $value;
	}
	return $self->{'sliceAdaptor'};
}

sub dbc {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( $value->isa("Adaptors::Connection") ) {
			$self->throw(   "Adaptor type should be Adaptors::Connection not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'dbc'} = $value;
	}
	return $self->{'dbc'};
}

sub type {
	my ( $self, $value ) = @_;
	if ($value) {
		unless (    lc($value) eq 'text'
				 or lc($value) eq 'hash'
				 or lc($value) eq 'object' )
		{
			$self->throw("Return types for queries must be one of 'text', 'hash' or 'object'\n"
			);
		}
		$self->{'type'} = $value;
	}
	return $self->{'type'};
}

sub columnList {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( ref($value) eq 'ARRAY' ) {
			$self->throw(
				   "columnLists must be array refs not " . ref($value) . "\n" );
		}
		$self->{'columns'} = $value;
	}
	return $self->{'columns'};
}

sub sampleHash {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( ref($value) eq 'HASH' ) {
			$self->throw(
				   "sample hash must be a hash ref not " . ref($value) . "\n" );
		}
		$self->{'samples'} = $value;
	}
	return $self->{'samples'};
}

sub analHash {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( ref($value) eq 'HASH' ) {
			$self->throw(
				 "analysis hash must be a hash ref not " . ref($value) . "\n" );
		}
		$self->{'analyses'} = $value;
	}
	return $self->{'analyses'};
}

sub buffer {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'buffer'} = $value;
	}
	return $self->{'buffer'};
}

sub sliceCursor {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'sliceCursor'} = $value;
	}
	return $self->{'sliceCursor'};
}

sub listCursor {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'listCursor'} = $value;
	}
	return $self->{'listCursor'};
}

sub end {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'end'} = $value;
	}
	return $self->{'end'};
}

sub sample {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		unless ( ref($value) eq "Bio::EnsEMBL::Variation::Individual" ) {

			# fetch it
			$value = $self->getSample($value);
		}
		$self->{'sample'} = $value;
	}
	return $self->{'sample'};
}

sub analysis {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		unless ( $value->isa("Bio::EnsEMBL::Analysis") ) {

			# fetch it
			$value = $self->getAnalysis($value);
		}
		$self->{'analysis'} = $value;
	}
	return $self->{'analysis'};
}

sub assembly {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'assembly'} = $value;
	}
	return $self->{'assembly'};
}

sub sampleKeyList {
	my ( $self, $sample, $value ) = @_;
	if ( defined $sample && $value ) {
		unless ( ref($value) eq 'ARRAY' ) {
			$self->throw(
					 "key lists must be array refs not " . ref($value) . "\n" );
		}
		$self->{'samplekeyList'}->{$sample} = $value;
	}
	return $self->{'samplekeyList'}->{$sample};
}

sub keyList {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		unless ( ref($value) eq 'ARRAY' ) {
			$self->throw(
					 "key lists must be array refs not " . ref($value) . "\n" );
		}
		$self->{'keyList'} = $value;
	}
	return $self->{'keyList'};
}

sub namespace {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'namespace'} = $value;
	}
	return $self->{'namespace'};
}

sub liftover {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'liftover'} = $value;
	}
	return $self->{'liftover'};
}

sub versions {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'versions'} = $value;
	}
	return $self->{'versions'};
}

# returns and caches an assembly mapper
sub AssemblyMapperAdaptor {
	my ($self) = @_;
	if ( defined $self->{'assembly_mapper'} ) {
		return $self->{'assembly_mapper'};
	}

	# go fetch one
	my $am =
	  $self->dbc->registry->get_adaptor( $self->dbc->species, 'core',
										 'assemblymapper' );
	$self->{'assembly_mapper'} = $am;
	return $self->{'assembly_mapper'};
}

# returns and caches an assembly mapper
sub CoordSystemAdaptor {
	my ($self) = @_;
	if ( defined $self->{'coord_system'} ) {
		return $self->{'coord_system'};
	}

	# go fetch one
	my $am =
	  $self->dbc->registry->get_adaptor( $self->dbc->species, 'core',
										 'coordsystem' );
	$self->{'coord_system'} = $am;
	return $self->{'coord_system'};
}

sub SliceAdaptor {
	my ($self) = @_;
	if ( defined $self->{'slice_adaptor'} ) {
		return $self->{'slice_adaptor'};
	}

	# go fetch one
	my $am =
	  $self->dbc->registry->get_adaptor( $self->dbc->species, 'core', 'slice' );
	$self->{'slice_adaptor'} = $am;
	return $self->{'slice_adaptor'};
}
1;
