#!perl
# Prototype variant coverage table Hbase adaptor by Simon White
package Modules::VariantCoverage;
use strict;
use warnings;
use Data::Dumper;
use Adaptors::Hbase;
use POSIX;
use Bio::EnsEMBL::Utils::Argument qw(rearrange);

use vars qw(@ISA);
@ISA = qw(Adaptors::Hbase);

# class variable used to trap error messages;
my $error = "";


sub new {
	my $caller = shift;
	my $class  = ref($caller) || $caller;
	my $self   = $class->SUPER::new(@_);
	my ( $scale, $bin, $threshold, $chunkSize,$table ) =
	  rearrange( [ 'SCALE', 'BIN', 'THRESHOLD', 'CHUNKSIZE','TABLE' ], @_ );

	# define default namespace
	$self->namespace('VARIANT_COVERAGE');
	$self->namespace($table) if $table;
	
	# need these factors to correctly
	# store and retreive compressed coverage
	$self->scale($scale);
	$self->bin($bin);
	$self->threshold($threshold);

	# init encoding
	$self->initCode;
	$self->initDecode;

	# set default chunk size
	$chunkSize = 1000 unless $chunkSize;
	$self->chunkSize($chunkSize);
	return $self;
}

sub storeStream {
	my ( $self, $fh ) = @_;
	#initialise bins
	$self->calculateBins();
	my @bins  = @{$self->compressionBins()};
	my $code =   $self->code;
	my $decode = $self->decode;
	my $start = 1;
	my $end   = 0;
	my $chr   = 0;
	my $total = 0;
	my $time  = time;
	my $lastPos = 0;
	my $lastStart = 1;
	my $cnt     = 0;
	my $lastDepth;
	my $depth;
	my $str;
	my $rowKey = "";
	my $count = 0;
	my $lookup = "";
	my $lookupCount = 0;
	my $lookupBatch = 1;

	# takes an stream as input
	# should contain chr start depth
	while (<$fh>) {
		chomp;
		$error = "";
		$cnt++;
		my @line = split("\t");

		
		# store the meta data using the HBASE adaptor
		
		
		if ( $line[1] == $lastPos + 1 ) {

			# we are just progressing sequentially
		} else {

			# gap - these should be set to zero
			# first print any previous data
			if ( $start && $end ) {
				my $c = $self->encodeCoverage( $bins[$lastDepth] );
				$error .= "BLOCK " . ( ( $end - $start ) + 1 ) . $c . "\n";
				$str .= ( $end - $start ) + 1 . $c;
				$end = 0;
				$count++;
			}

			# print the gap
			# gap has special code -1
			# reserved only for gaps
			$error .=
			  "GAP  " . ( ( $line[1] - $lastPos ) - 1 ) . $code->[-1] . "\n";
			$str .= ( ( $line[1] - $lastPos ) - 1 ) . $code->[-1];
			$lastDepth = 0;
			$start     = $line[1];
		}
		if ( $bins[ $line[2] ] == $lastDepth ) {
			$start = $lastPos unless $start > 1;
			$end = $line[1];
		} else {
			if ( $start && $end ) {
				my $kvs->{'D'} = "1";
				#store( $rk, $kvs );
				my $c = $self->encodeCoverage($lastDepth);
				$error .=
				    "BLOCK size "
				  . ( ( $end - $start ) + 1 )
				  . " Last depth  $lastDepth  encoded to $c " . "\n";
				$str .= ( ( $end - $start ) + 1 ) . $c;

				# now decode it and see if it is right
				my $n = $self->decodeCoverage($c);
				$error .= "- $n\n";
				unless ( $lastDepth == $n ) {
					print STDERR "$error\n";
					$self->throw("What the fook $lastDepth != $n");
				}

				# reset for the next one
				$start = $line[1];
				$end   = $line[1];
				$count++;
			}
		}
		$lastDepth = $bins[ $line[2] ];
		$lastPos   = $line[1];
		$end       = $line[1];

		if ( $chr ne $line[0] ) {

			# last row
			if ($start) {
				my $s = sprintf "%09d", $start;
				my $e = sprintf "%09d", $line[1];
				#store( $rk, $kvs );
				$count++;
			}
			$start = 0;
			$end   = 0;
		}
		$chr = $line[0];
		if ( $count >= $self->chunkSize ) {
			$total += $count;
			my $s = time - $time;
			$s += 1;    # stop division by zero
			my $num = sprintf( "%2d", $total / $s );
			print STDERR
"\n\nStored $total blocks in $s seconds ( $num blocks per second )\r";
			my $pos = sprintf "%09d",  $line[1];
			$lookup .= $self->storeData($line[0],$lastStart,$line[1],$str) ;
			$lookupCount++;
			$lastStart = $line[1];
			$str    = "";
			$count  = 0;
		}
		if ($lookupCount >= $self->chunkSize){
			# store the lookup
			$self->storeLookup($lookup,$lookupBatch);
			$lookupBatch++;
			$lookup = "";
		}
	}
	# catch end of block
	if ( $start && $end ) {
		my $c = $self->encodeCoverage($lastDepth);
		$error .=
		    "BLOCK size "
		  . ( ( $end - $start ) + 1 )
		  . " Last depth  $lastDepth  encoded to $c " . "\n";
		$str .= ( ( $end - $start ) + 1 ) . $c;
		my $s = sprintf "%09d", $start;
		my $e = sprintf "%09d", $end;
		my $kvs->{'D'} = "Covered";
		$self->storeData($chr,$lastStart,$end,$str);
	}
	# catch last lookup keys
	if ( $lookup ){
		# store the lookup
		$self->storeLookup($lookup,$lookupBatch);
	}
	# store the meta data associated with this
	# then we can always look up the relevant data
	my $meta = "bin=".$self->bin.",scale=".$self->scale.",threshold=".$self->threshold;
	$self->storeMeta($meta);
	# store the lookup
	
	return;
}



sub fetchBySlice {
	my ($self,$slice) = @_;
	# parse meta data
	my $meta = $self->getMeta();
	my ($bin,$scale,$threshold) = $meta =~ /bin=(\d+),scale=(\d+),threshold=(\d+)/;
	$self->throw("cannot parse bin scale and threshold out of meta data $meta\n")
		unless ($bin && $scale && $threshold );
	$self->bin($bin);
	$self->scale($scale);
	$self->threshold($threshold);	
	# calculate the bins
	$self->calculateBins();
	my $bins = $self->decompressionBins();
	unless ( $slice->isa("Bio::EnsEMBL::Slice") ) {
		$self->throw(   "Adaptor type should be a::EnsEMBL::Slice not ". ref($slice) . "\n" );
	}
	print STDERR "SLICE " . $slice->start ."\n";
	my ($iterator) = $self->fetchBySampleSlice($self->sample->name,$slice) ;
	if ($iterator){
	DATA:	while (my $data = $iterator->nextList){
			foreach my $hash (@{$data}) {
				# now we decompress and trim to slice
				my $d = $self->parseRowKey($hash);
				my $pos = $d->{'START'};
				# get rid of zero padding
				$pos =~ /^0*/;
				my $compressedStr = $hash->{'columns'}->{'D:'}->{'value'};
				# split the string
				my @pieces = ( $compressedStr =~ /(\d+\D+)/g );
  		PIECE:	foreach my $piece (@pieces) {
    				my ($length,$match) = ( $piece =~ /^(\d+)(\D+)/ );
    				print "piece $length $match\n";
    				if ( $match eq $self->code->[-1] ){
    					# last char is reserved for a gap - don't return it
    					$pos += $length;
    					next PIECE;
    				}	
    				# trim to slice
    				$pos += $length;
    				#print "$pos\t-\t" .$slice->start ."\n"; 
    				next PIECE if $pos < $slice->start;
    				last DATA if $pos > $slice->end;
    				print $d->{"CHR"} ."\t$pos\t" ;
    				my $decompressed =  $self->decodeCoverage($match);
    				print $bins->[$decompressed] ."\n";

  				} 
			}
		}
	}
}

sub calculateBins {
	my ($self) = @_;

	# use the bin and the scale factor to determine
	# which bin the coverage lies in.
	# ie a bin of 5 and sf of 2:
	# bin	number
	#	1	0-5
	#	2	5-15
	#	3	15-25
	# calculate the bins and store them in an array 0 - 100,000
	my $threshold = $self->threshold;
	my $scale     = $self->scale;
	my $bin       = $self->bin;
	my @cBins;
	my @dBins;
	my $ccnt= 0;
	$self->throw(
		"Cannot make bins without a the following being defined:
	threshold - $threshold 
	binsize	  - $bin
	scale     - $scale\n"
	  )
	  unless ( $scale && $threshold && $bin );
	my $currBin = 0;
	for ( my $i = 0 ; $i <= 10000000 ; $i++ ) {

		unless ( $i <= $currBin * $bin ) {
			if ( $i >= $threshold ) {

				# scale up
				$bin *= $scale;
				
				# find the average of the decompression bins
				$dBins[$currBin] = $dBins[$currBin] / $ccnt;
				$ccnt = 0;				
			}
			$currBin++;
		}
		$cBins[$i] = $currBin;
		$dBins[$currBin] += $i;
		$ccnt++;
	}
	$self->compressionBins( \@cBins );
	$self->decompressionBins( \@dBins );	
	return;
}

sub encodeCoverage {
	my ( $self, $num ) = @_;
	my $code = $self->code;
	# break down the number into base 209
	$error .= "Ecoding $num \n";
	if ( $num < $#$code ) {

		# single char
		$error .= $code->[$num] . " $num \n";
		print STDERR "ERROR $error \n" if $num == 80;
		return $code->[$num];
	}

	# this is more complicated
	# have to convert to base
	# scalar(@$code) to make multi char
	# values
	my @digits;
	my $size = $#$code;
	$error .= "$num and $size\n";
	while ( $num >= $size ) {
		my $val       = $num / $size;
		my $rnd       = floor($val);
		my $remainder = $num - ( $rnd * $size );
		$error .= "GOT val $val rounded $rnd remainder $remainder\n";
		push( @digits, $remainder );
		$num = $rnd;
	}
	push( @digits, $num ) if defined($num);

	# now convert the digits into characters
	my $str;
	$error .= "Have an array of size " . scalar(@digits) . "\n";
	$error .= "Got this to encode: ";
	while ( scalar(@digits) > 0 ) {
		my $d = pop(@digits);
		$error .= "$d - ";
		$str   .= $code->[$d];
		$error .= $code->[$d] . " $d \n";
	}
	print STDERR "ERROR $error \n" if $num == 80;
	$error .= " = $str\n";
	return $str;
}

sub decodeCoverage {
	my ( $self, $str ) = @_;
	my $decode = $self->decode();
	my $code = $self->code();	
	# break down the number into base 209
	$error .= "Decoding STRING $str\n";
	my $total = 0;
	my @digits;

	# using substr as split seems to mess up some of the ascii codes somehow
	for ( my $i = 0 ; $i < length($str) ; $i++ ) {
		my $c = substr( $str, $i, 1 );
		push @digits, $decode->{$c};
		$error .= "GOT " . $decode->{$c} . " from $c is that right?\n";
	}
	while ( my $n = shift(@digits) ) {
		my $multiplier = scalar(@digits);
		if ( scalar(@digits) > 0 ) {
			$total += $n * ( scalar(@digits) * $#$code );
		} else {

			# last digit
			$total += $n;
		}
	}
	return $total;
}

sub initCode {
	my ($self) = @_;
	my @code = ( '!', '#', '$', '%', '&', '(', ')', '*', '+', ',', '-', '.',
				 '/', ':', ';', '<', '=', '>', '?', '@', 'A', 'B', 'C', 'D',
				 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
				 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', ']',
				 '^', '_', '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
				 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
				 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~');
	$self->code( \@code );
	return;
}

sub initDecode {
	my ($self) = @_;
	my $code = $self->code();
	# look up for conversion the other way
	my $c = $self->code();
	my %hash;
	for ( my $i = 0 ; $i < scalar(@$code) ; $i++ ) {
		$hash{ $c->[$i] } = $i;
	}
	$self->decode( \%hash );
	return;
}
##########CONTAINERS##########
sub scale {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'scale'} = $value;
	}
	return $self->{'scale'};
}

sub bin {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'bin'} = $value;
	}
	return $self->{'bin'};
}

sub threshold {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'threshold'} = $value;
	}
	return $self->{'threshold'};
}

sub chunkSize {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'chunkSize'} = $value;
	}
	return $self->{'chunkSize'};
}

sub compressionBins {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'compressionBins'} = $value;
	}
	return $self->{'compressionBins'};
}

sub decompressionBins {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'decompressionBins'} = $value;
	}
	return $self->{'decompressionBins'};
}

sub code {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'code'} = $value;
	}
	return $self->{'code'};
}

sub decode {
	my ( $self, $value ) = @_;
	if ( defined $value ) {
		$self->{'decode'} = $value;
	}
	return $self->{'decode'};
}
1;
