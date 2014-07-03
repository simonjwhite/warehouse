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
  $self->throw("No table defined") unless $table;
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
		my $row = $self->parseRowKey($data);
		my $cols = $self->parseColumns($data);
		my $slice = $sa->fetch_by_region('toplevel',$row->{CHR});

		# build a ensembl variation object
		my $vf = Bio::EnsEMBL::Variation::VariationFeature->new
       (-start   => $row->{START},
        -end     => $row->{START},
        -strand  => 1,
        -slice   => $slice,
        -allele_string => $cols->{REF} . "/".$cols->{ALT});
        $output = $vf;
	} elsif ($format eq 'text'){
		my $string;
		# make the VCF format
		# 1 read the header
		my $mapping = $self->mapping;
		unless ( $mapping){
			my $header = $self->getHeader($self->sample->name);
			# parse the header tags
			my @lines = split("\n",$header);
			foreach my $line ( @lines){
				$string .= "$line\n";
				if ( $line =~ /^##FORMAT=<ID=(\w+)\,/){
					push (@{$mapping->{FORMAT}},$1);
				}
				if ( $line =~ /^##INFO=<ID=(\w+)\,/){
					push(@{$mapping->{INFO}},$1);
				}
			}
			$self->mapping($mapping);
		}
		my $rk = $self->parseRowKey($data);
		my $cols = $self->parseColumns($data);
		my $id = ".";
		my $qual = ".";
		$id = $cols->{'ID'} if $cols->{'ID'} ;
		$qual =  $cols->{'QUAL'} if  $cols->{'QUAL'};
		$string .= $rk->{'CHR'}  ."\t".
				   $rk->{'START'} ."\t". 
				   $id."\t".
				   $cols->{'REF'} ."\t".
				   $cols->{'ALT'} ."\t".
				   $qual ."\t".
				   $cols->{'FILTER'} ."\t";
		#INFO
		my $info;
		foreach my $tag ( @{$mapping->{INFO}}){
			if ( $cols->{$tag} ){
				$info .= $cols->{$tag} .";";	
			} else {
				$info .= ".";	
			}

		}
		$info =~ s/;$//;
		$string .= $info ."\t";
		# FORMAT
		my $format;
		foreach my $tag ( @{$mapping->{FORMAT}}){
			$format .= $tag.":";
		}
		$format =~ s/:$//;	
		$string .= $format ."\t";	
		$format = "";   
		foreach my $tag ( @{$mapping->{FORMAT}}){
			if ( $cols->{$tag} ){
				$format .= $cols->{$tag}.":";
			} else {
				$format .= ".:";	
			}
		}
		$string .= $format ."\n";			   
		$output = $string;
	} else {
		return $self->SUPER::format($data);
	}
	return $output;
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

sub mapping {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'mapping'} = $value;
	}
	return $self->{'mapping'};
}
1;