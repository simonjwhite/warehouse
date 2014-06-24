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
  	$self->type('hash');
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

	# header
	my $header = $self->getHeader($sample);
	print "$header";
			
	my $iterator = $self->fetchAllBySample($sample);
	while (my $data = $iterator->nextList){
	  # now loop through them line by line
	  # fortunately the keys are sorted
	  return unless $data;
	  foreach my $hash (@{$data}) {
	  		my $d = $self->parseRowKey($hash);
			my $chr = $d->{"CHR"};
			my $pos = $d->{"START"};

			# remove 0 padding from numbers
			$pos =~ s/^0+//;
			# columns
			my $ch = $hash->{'columns'};
			print "$chr\t$pos\t";
			my $line =  $ch->{"D:"}->{'value'};
			print "$line\n";
		}
	}
}


1;