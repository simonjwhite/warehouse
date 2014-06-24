#!perl

package Modules::SliceGVCF;

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
	# need to do this on whole chromosome slices for it 
	# to work properly
	my %coverageHash;
	if ($self->coverage){
		print STDERR "Fetching coverage data - using slices starting at position 1 to avoid slice boundary problems\n";
		my $ca = $self->getVariantCoverageAdaptor('hash',$self->buffer);
		$ca->namespace($self->coverageTable) if $self->coverageTable;
		# set the samples we want to use ortherwise use all of them
		$ca->sampleHash($self->sampleHash) if $self->sampleHash;
		# make a new slice that always starts at 1 
		# so we eliminate any boundary overlap issues
		my $sa = $slice->adaptor;
		my $newSlice = $sa->fetch_by_region('toplevel',$slice->seq_region_name,1,$slice->end);
		print STDERR "Got new slice " . $newSlice->name ."\n";
		my $iterator = $ca->fetchBySlice($newSlice);
		while (my $data = $iterator->nextSlice){
			# test 
			foreach my $hash (@{$data}) {
				my $d = $self->parseRowKey($hash);
				my $sam = $d->{"SAMPLE"};
				# as we are fetching by slice we dont need chr
				my $start = $d->{"START"};
				my $end = $d->{"END"};
				push (@{$coverageHash{$sam}},"$start,$end");
			}
		}
	}
	
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
				$self->print(\%lineHash,$ref,$alt,$format,\@samples,\%coverageHash);
				# empty the hash
				%lineHash = ();
			}
			# build all the hash values for this line
			$lineHash{$chr."\t".$pos."\t.\t".$ref."\t"}->{$sam} = $line;
			$last_pos = $pos;
		}
	# print the last line	
	$self->print(\%lineHash,$ref,$alt,$format,\@samples,\%coverageHash);
	}
}

sub print {
	my ( $self, $lineHash,$ref,$alt,$format,$samples,$coverageHash ) = @_;
	# get the lineHash
	foreach my $rowkey ( keys %{$lineHash} ){
		my $string = "";
		my $pass = 0;
		my $fmt;
		$string .= "$rowkey";
		$string .= "$alt\t60\tPASS\t.\t$format..VT.FT.AA\t";
		my $data = $lineHash->{$rowkey};
		my @tmp =  split("\t",$rowkey);
		my $pos = $tmp[1];
		#print STDERR "Got position $pos\n";
		foreach my $sample (@$samples){
			if ($data->{$sample}){
				my $hash = $self->parseLine($data->{$sample});
				$string.=  $hash->{'SAMPLE'}.":";
				$string.=  $hash->{'QUAL'}.":";
				$string.=  $hash->{'FILTER'}.":";
				$string.=  $hash->{'ALT'}."\t";										
				$pass++ if $hash->{'FILTER'} eq "PASS";
			} else {
				if ( defined $coverageHash){
					# check to see if it has data and therefore is no var

					my $list = $coverageHash->{$sample};
					my $result = ".:0:0:0:.:.:No_data:.\t";
					if ($list){
						for ( my $ i = 0 ; $i < scalar(@$list) ; $i++ ){
							my $block = $list->[$i];
							my ($s,$e) = split(",",$block);
							#print  "BLOCK $pos $sample START END $s $e\n";
							if ($e < $pos){
								# get rid of it - the keys are sorted if we dont need it now
								# we will never need it
								shift(@$list);
								$i--;
								$coverageHash->{$sample} = $list;
								next;
							}
							last if $s > $pos;
							if ( $pos >= $s && $pos <= $e){
								# we are covered
								#print STDERR "COVERED $pos START END $s $e\n";
								$result = "0/0:0:6:6:.:.:No_var:.\t";
								last;
							}
						}
					}
					$string .= $result;
				} else {
					$string .= ".:0:0:0:.:No_data:.\t";
				}
			}	
		}
		$string =~ s/\t$//;
		print "$string\n";# if $pass;
	}
	return;
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