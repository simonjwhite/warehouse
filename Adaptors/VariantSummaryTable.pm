#!perl
# Prototype variant table Hbase adaptor by Simon White

package Adaptors::VariantSummaryTable;

use strict;
use warnings;
use Data::Dumper;
use Adaptors::Hbase;
use Bio::EnsEMBL::Variation::VariationFeature;
use Bio::EnsEMBL::Utils::Argument qw(rearrange);

use vars qw(@ISA);

@ISA = qw(Adaptors::Hbase);



sub new {
  my $caller = shift;
  my $class = ref($caller) || $caller;
  my $self = $class->SUPER::new(@_);
  # define namespace
  my $table = 'VARIANTSUMMARY';
  $self->namespace($table);
  return $self;
}

# override format method from base class to handle matching headers to the columns
sub format {
	my ($self,$data)= @_;
	my $format = $self->type;
	my $output;
	#print STDERR "Handling hash creation in inherited class\n";
	if ( $format eq 'hash'){
 	# need to get the header
 	# do we already have it stored?
 	unless ($self->header){
		# fetch it and store it
		my $h = $self->getHeader($self->sample->name);
		$self->throw("Cannot find header\n")
 			unless ($h);
		# store it
		my @array = split("\t",$h);
	 		$self->header(\@array);
	 }
	my $headerString = $self->getHeader($self->sample->name);
	my @header = split("\t",$headerString);
	# parse the data out of the hash
	my $row = $data->{'row'};
	my ($cs,$asm,$chr,$start,$end) = split(":",$row);
	my $string =  $data->{'columns'}->{'D:'}->{'value'};
	my @array = split("\t",$string);
	$self->throw("Cannot match up header with columns")
		unless(scalar(@array) == scalar(@header));	

		# build the hash out of the data
		for ( my $i = 0 ; $i < scalar(@header); $i++){
			$output->{$header[$i]} = $array[$i];
			#print STDERR "GOT $row " . $header[$i] . " = " . $array[$i] ."\n";
		}
		$output->{'chr'} = $chr;
		$output->{'cs'}  = $cs;
		# strip zero padding
		$start =~ s/^0+//;
		$output->{'start'} = $start;
		$output->{'end'} = $end;	
	} else {
		return $self->SUPER::format($data);
	}
	return $output;
}



##################
####Containers####
##################

sub header {
	my ( $self, $value ) = @_;
	if (defined  $value) {
		unless (ref($value) eq 'ARRAY'){
			$self->throw("header must be array refs not " . ref($value) ."\n");
		}
		$self->{'header'} = $value;
	}
	return $self->{'header'};
}


1;