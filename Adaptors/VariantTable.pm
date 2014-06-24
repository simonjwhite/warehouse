#!perl
# Prototype variant table Hbase adaptor by Simon White

package Adaptors::VariantTable;

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
  my ( $vt,$table) =
	  rearrange( [ 'VARTYPE','TABLE' ], @_ );
 
  $self->varType($vt) if $vt;
  # define namespace
  $table = 'VARIANT' unless $table;
  $self->namespace($table);
  return $self;
}

sub varTest {
	my ($self,$ref,$alt) = @_;
	if ($self->varType){
	if ( $self->varType eq "snp" ){
		return 1 if length($ref) + length($alt) == 2;
	}
	if ( $self->varType eq "indel" ){
		return 1 if length($ref) + length($alt) > 2;
	}
	} else {
		print STDERR "No variant type defined \n";
	}
	return 0;
}


# override format method from base class to handle object creation
sub format {
	my ($self,$data)= @_;
		my $format = $self->type;
	my $output;
	if ($format eq 'object'){
		print "Handling object creation in inherited class\n";
		my $sa = $self->SliceAdaptor;

		# parse the data out of the hash
		my $row = $data->{'row'};
		my ($cs,$asm,$chr,$start,$end) = split(":",$row);
		my $slice = $sa->fetch_by_region('toplevel',$chr);
		my $string =  $data->{'columns'}->{'D:'}->{'value'};
		my ($id,$ref,$alt,$qual,$filter,$info,$format,$sample) = split("\t",$string);
		# build a ensembl variation object
		my $vf = Bio::EnsEMBL::Variation::VariationFeature->new
       (-start   => $start,
        -end     => $start,
        -strand  => 1,
        -slice   => $slice,
        -allele_string => "$ref"."/"."$alt");
        $output = $vf;
	} else {
		return $self->SUPER::format($data);
	}
	return $output;
}

sub parseLine {
	my ($self,$line) = @_;
	# return a hash ref
	my @array = split("\t",$line);
	my $hr;
	$hr->{'ID'} 	= $array[0];
	$hr->{'REF'} 	= $array[1];	
	$hr->{'ALT'} 	= $array[2];	
	$hr->{'QUAL'} 	= $array[3];
	$hr->{'FILTER'} = $array[4];
	$hr->{'INFO'} 	= $array[5];	
	$hr->{'FORMAT'} = $array[6];
	$hr->{'SAMPLE'} = $array[7];
	return $hr;
}


sub varType {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->throw("Variant types must be one of snp or indel not $value")
			unless (lc($value) eq 'snp' or lc($value) eq 'indel');
		$self->{'vt'} = lc($value);
	}
	return $self->{'vt'};
}

1;