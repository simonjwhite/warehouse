#!perl
=head1 CONTACT

  Please email comments or questions to simow@bcm.edu

=cut

=head1 NAME: 
	SliceVS	

	Used for scanning slices of a variant summary table

=cut
package Modules::SliceVS;

use strict;
use warnings;
use Data::Dumper;
use Bio::EnsEMBL::Utils::Exception qw(throw);
use Bio::EnsEMBL::Utils::Argument qw(rearrange);

use vars qw(@ISA);
@ISA = qw(Adaptors::VariantTable);

sub new {
	my ( $class, @args ) = @_;
    my $caller = shift;
  	$class = ref($caller) || $caller;
  	my $self = $class->SUPER::new(@_);
  	my ( $table) =
	  rearrange( [ 'TABLE' ], @_ );
  	$self->type('hash');
  	# override the superclass namespace if defined
  	$self->namespace($table);
	return $self;
}


sub summaryFromSlice {
	my ( $self, $slice ) = @_;

	# get the variant adaptor
	my $va = $self->getVariantAdaptor('hash',$self->buffer);
	# 
	$va->namespace($self->namespace);
		
	# fetch the objects
	my $iterator = $va->fetchBySlice($slice);
	while (my $data = $iterator->nextSlice){
	  # now loop through them line by line
	  # fortunately the keys are sorted
	  return unless $data;
	
	  # list the samples in the order we want them 

	  foreach my $hash (@{$data}) {
		my $d = $self->parseRowKey($hash);
		my $sam = $d->{"SAMPLE"};
		my $chr = $d->{"CHR"};
		my $pos = $d->{"START"};
		
		# columns
		my $ch = $hash->{'columns'};
		my $line =  $ch->{"D:"}->{'value'};

	}
}


###########CONTAINERS###########



1;