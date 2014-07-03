#!perl

package Modules::SampleVCF;

use strict;
use warnings;
use Data::Dumper;
use Bio::EnsEMBL::Utils::Exception qw(throw);
use Bio::EnsEMBL::Utils::Argument qw(rearrange);

use vars qw(@ISA);
@ISA = qw(Adaptors::VariantTable);



sub new {
    my $caller = shift;
  	my $class = ref($caller) || $caller;
  	my $self = $class->SUPER::new(@_);
  	my ( $table ) =
	  rearrange( [ 'TABLE' ], @_ );
  	# override the superclass namespace if defined
  	$self->namespace($table);
  	# want to return a hash object
  	$self->type('text');
	return $self;
}

sub vcfFromSample {
	my ( $self, $sample ) = @_;
	# get the variant adaptor
	# fetch the objects
	my $list;
	# need to fetch the keylist out of the database
	# lists are split into sections, generate the keys to 
	# pull down each section untill you reach the end

	my $iterator = $self->fetchAllBySample($sample);
	while (my $data = $iterator->nextList){
		foreach my $line (@$data) {
			print $line;
		}
	}
}



1;