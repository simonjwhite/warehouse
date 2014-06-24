#!perl

package Modules::SliceVCF;

use strict;
use Modules::Vcf;
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
  	my ( $table,$coverage,$ct ) =
	  rearrange( [ 'TABLE','COVERAGE','COVERAGETABLE' ], @_ );
  	$self->type('hash');
  	# override the superclass namespace if defined
  	$self->namespace($table);
  	$self->coverageTable($ct) if $ct;
  	$self->coverage($coverage) if $coverage;
	return $self;
}

sub varCountFromSlice {
	my ($self,$slice) = @_;
	# get the variant adaptor
	my $va = $self->getVariantAdaptor('hash',$self->buffer);
 
	$va->namespace($self->namespace);
	# set the samples we want to use ortherwise use all of them
	$va->sampleHash($self->sampleHash) if $self->sampleHash;
	my $iterator = $va->fetchBySlice($slice);
	my $lastPos = 0;
	my $count = 0;
	my $chr;
	while (my $data = $iterator->nextSlice){
	  # now loop through them line by line
	  # fortunately the keys are sorted
	  return unless $data;
	  foreach my $hash (@{$data}) {
		my $d = $self->parseRowKey($hash);
		my $sam = $d->{"SAMPLE"};
		$chr = $d->{"CHR"};
		my $pos = $d->{"START"};
		if ( $lastPos && $pos == $lastPos ){
			$count++;
		} else {
			if ( $count ){
			print "$chr\t$lastPos\t$count\n";
			}
			$count = 1 ;
			$lastPos = $pos;
		}
	  }
	}
	print "$chr\t$lastPos\t$count\n";
	return;
}

sub vcfFromSlice {
	my ( $self, $slice ) = @_;

	# get the variant adaptor
	my $va = $self->getVariantAdaptor('hash',$self->buffer);
	# 
	$va->namespace($self->namespace);
	
	# set the samples we want to use ortherwise use all of them
	$va->sampleHash($self->sampleHash) if $self->sampleHash;
	

	# do we want coverage? if so pre-load the 
	# coverage data for that slice and sample set.
	
	# fetch the objects
	my $iterator = $va->fetchBySlice($slice);
	while (my $data = $iterator->nextSlice){
	  # now loop through them line by line
	  # fortunately the keys are sorted
	  return unless $data;
	
	  # list the samples in the order we want them 
	  # to appear in the pvcf
	  # ie: sorted by name
	  my %sh = %{$self->sampleHash};
	  my @samples = sort { $sh{$a}->name cmp $sh{$b}->name } keys %sh;
	  # header
	  print "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";
	  foreach my $sample (@samples){
	  	  # hash is dbIDs need to print the name
	  	  print "\t".$sh{$sample}->name;
	  } 
	  print "\n";
	  $self->throw("Need at least 2 samples to make a pVCF")
		  unless ( scalar(@samples) > 1);

		# work line by line
		my $last_pos;
		my %lineHash;
		my $format;
		my $ref;
		my $alt;
		foreach my $hash (@{$data}) {
			my @alts;
			my $d = $self->parseRowKey($hash);
			my $sam = $d->{"SAMPLE"};
			my $chr = $d->{"CHR"};
			my $pos = $d->{"START"};
	
			
			# columns
			my $ch = $hash->{'columns'};
			my $line =  $ch->{"D:"}->{'value'};
			my $hr = $self->parseLine($line);
			$ref = $hr->{'REF'};
			$alt = $hr->{'ALT'};
			# limit to the variants we want
			if ($self->varType){
				#print STDERR "limiting to type " . $self->varType ."\n";
		     	next unless $self->varTest($ref,$alt) ;
			}
			
			$format = $hr->{'FORMAT'};
			# remove trailing 0s
			$pos =~ s/^0+//;
			if ( $last_pos && $pos ne $last_pos ){
				$self->print(\%lineHash,\@samples);
				# empty the hash
				%lineHash = ();
			}
			# build all the hash values for this line
			$lineHash{$chr."\t".$pos}->{$sam} = $line;
			$last_pos = $pos;
		}
	# print the last line	
	$self->print(\%lineHash,\@samples);
	}
}

sub print {
	my ( $self, $lineHash,$samples ) = @_;
	# This is all the entries for a single row
	# we need to acertain how many refs and alts we have 1st
	my %varHash;
	my $vcfParser = Modules::Vcf->new();
	my (%refs,%alts);
	foreach my $rowkey ( keys %{$lineHash} ){
		my $data = $lineHash->{$rowkey};
		foreach my $sample (@$samples){
			if ($data->{$sample}){
				my $hash = $self->parseLine($data->{$sample});
				$hash->{'ALT'} =~ s/<NON_REF>//;	
				foreach my $ref ( split(",",$hash->{'REF'} )){
					$refs{$ref} = 1 unless $refs{$ref};
				}
				foreach my $alt ( split(",",$hash->{'ALT'} )){
					$alts{$alt} = 1 unless $alts{$alt};
				}
			}
		}
		return unless %alts;
		# keep the hash of the rows with variants
		$varHash{$rowkey} = $data;
		print STDERR "Worked on row $rowkey - got these refs :";
		print STDERR join( "," , keys %refs);
		print STDERR "\n";
		print STDERR " got these alts :";
		print STDERR join( "," , keys %alts);
		print STDERR "\n";	
		# so the hash key order is now the official ordering of the 
		# refs and alts, now I have to convert that from the sample
		# ordering to the multisample ordering.	
	}
	# figure out the numbering of the refs and alts for the enire row
	my $cnt = 0 ;
	print "ROW ORDERING\n";
	my %alleles;
	foreach my $key ( sort keys %refs ){
		next if $key eq "";
		$alleles{$key} = $cnt;
		print "ref $key - $cnt \n";
		$cnt++;
	}
	foreach my $key ( sort keys %alts ){
		next if $key eq "";
		$alleles{$key} = $cnt;
		print "alt $key - $cnt \n";
		$cnt++;
	}	
	
	# loop through the hash again where we have variants
	foreach my $rowkey ( keys %varHash ){
		print "ROW $rowkey\n";
		my $data = $lineHash->{$rowkey};
		foreach my $sample (@$samples){
			if ($data->{$sample}){
				my $index;
				my $hash = $self->parseLine($data->{$sample});
				if ( $hash->{'ALT'} eq "<NON_REF>" ){
					# we dont need to figure out the genotype for this one
					print "This is a gap block " . $hash->{'SAMPLE'} ."\n";
					next ;
				}	
				$hash->{'ALT'} =~ s/<NON_REF>//;
				# we need to identify what the sample numbering was 
				# and then modify it to fit our multi sample numbering
				print  Dumper($hash);
				# find the GT field
				$index =  $vcfParser->get_tag_index($hash->{'FORMAT'},'GT',':');
				my @array = split(":",$hash->{'SAMPLE'});
				print "Genotype " . $array[$index] ."\n"; 
				my ($ref,$alt) = split(/\/|\|/,$array[$index]);
				print "REF $ref ALT $alt\n";
				my $snum = $self->gtOrder($hash->{'REF'}.",".$hash->{'ALT'});
				print "REF " . $snum->{$ref} ."  ALT " . $snum->{$alt} ."\n";
				# and in row terms that is
				
				print "ROW REF " . $alleles{$snum->{$ref}} . " ALTS " . $alleles{$snum->{$alt}} ." \n";
			}
		}		
	}
	return;
}

sub gtOrder {
	my ($self,$list) =@_;
		my %hash;
		print "LIST $list\n";
		my @array = split(/,/,$list);
		for ( my $i = 0 ; $i < scalar(@array) ; $i++){
			$hash{$i} = $array[$i];
		}
	return \%hash;
}

###########CONTAINERS###########

sub coverage {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'coverage'} = $value;
	}
	return $self->{'coverage'};
}
sub coverageTable {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'coverageT'} = $value;
	}
	return $self->{'coverageT'};
}


1;