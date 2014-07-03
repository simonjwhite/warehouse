#!/usr/bin/perl
#
#Author:Simon White simonw@bcm.edu
#
use strict;
use warnings;
use Carp;
use Data::Dumper;
use Vcf;
use Getopt::Long;
use Adaptors::Hbase;
use Env qw(REGISTRY);


my $registry = $REGISTRY;
my $tags;
my $hbase;
my $table;
my $upload;

# Vcf-tools filter for selecting lines from a file based on the filter feild
# will also replace or remove 'Dummy' tags used in the custom filters.
my $usage = "zcat file.vcf.gz | importAnnotationHbase.pl
-tags    comma separated list of INFO tags to output.
         if a file is provided it will take that instead
         just write a list of tags separated by commas or newlines
-table	 table name
-upload  do the upload
";
&GetOptions(
			 'tags:s' 	=> \$tags,
			 'table:s'	=> \$table,
			 'upload!'	=>\$upload
);
my @tags;

my $hbase;
# open the connection
my $db = Adaptors::Connection->new( -host => "hadoop-headnode1",
							   -port => "9090",
							   -registry => $registry );

$hbase = Adaptors::Hbase->new(
								-con => $db,
								 );
$hbase->namespace($table);

# parse the tags
#is it a file
if ( -e $tags ) {

	#open the file
	open( TAGS, $tags ) or die("Cannot open file $tags\n");
	while (<TAGS>) {
		chomp;
		my @line = split( /,/, $_ );
		push @tags, \@line;
	}
} else {
	foreach my $t ( split( /,/, $tags ) ) {
		push @tags, [$t];
	}
}
die($usage) unless ($tags);

# Open VCF file and add all required header lines
my $opts;
my %args = ( print_header => 1 );
if ( $$opts{region} )         { $args{region} = $$opts{region}; }
if ( exists( $$opts{file} ) ) { $args{file}   = $$opts{file}; }
else { $args{fh} = \*STDIN; }
my $vcf = Vcf->new(%args);
$$opts{vcf} = $vcf;
$vcf->parse_header();
my $time = time;
#1st print the header lines
	my $hstr;
foreach my $tag (@tags) {
	# if we have multiple comma separated tags on in the file
	# show all tags and all descriptions
	if ( scalar(@$tag) > 0 ) {
		my $comma = "";
		foreach my $t (@$tag) {

		}
		foreach my $t (@$tag) {
			my @header = @{ $vcf->get_header_line( key => 'INFO', ID => $t ) };
			my $h = $header[0];
			if ($h) {
				my $d = $h->{Description};
				$d =~ s/\s+/_/g;
				#print "," . $d;
				$hstr .=  "##INFO=<ID=" . 
				$h->{ID}.",Number=" . $h->{Number}.",Type=" .
				$h->{Type}.",Description=\"" .$h->{Description} ."\">\n";			
			}
		}
	} else {
		my @header = @{ $vcf->get_header_line( key => 'INFO', ID => $tag ) };
		my $h = $header[0];
		if ($h) {
			print $h->{Description} . " ($tag)\t";
			print Dumper($h);
			exit;
		} else {
			print "$tag\t";
		}
	}

}
print "$hstr \n";
				$hbase->_store('INFO',$hstr);	
my $count = 0;
my $totalV = 0;
while ( my $line = $vcf->next_line() ) {
	my $x = $vcf->next_data_array($line);
	#print STDERR "LINE $line\n";
	# print the row key
	# ignore SVs
	if ( $x->[7] =~ /VT=(\S+);*/){
		#print "DOLLAH ONW $1\n";
		next unless $1 eq 'SNP' or $1 eq 'INDEL';
	}
	# remember 0 padding
	my $start = sprintf("%09d",$x->[1]);
	my $rk = "c:GRCh37:" . $x->[0] . ":" . $start . ":" . $x->[3] . ":";
	my @alts = split( ",", $x->[4] );
	for ( my $i = 0 ; $i < scalar(@alts) ; $i++ ) {
		my $str = "";
		foreach my $tag (@tags) {
			my $result;
			if ( scalar(@$tag) > 0 ) {
				my %hash;
				foreach my $t (@$tag) {
					my $e = $vcf->get_info_field( $x->[7], $t );
					if ($e) {
						$e = "$t=" . $e ;
						$hash{$e} = 1;
					}
				}
				foreach my $r ( keys %hash ) {
					$result .= "," if $result;
					$result .= "$r";
				}
			} else {
				$result = $vcf->get_info_field( $x->[7], $tag );
			}
			unless ($result) {

				# try in the other feilds
				foreach my $d (@$x) {
					$result = $x->[0] if $tag->[0] eq "CHROM";
					$result = $x->[1] if $tag->[0] eq "POS";
					$result = $x->[3] if $tag->[0] eq "REF";
					$result = $x->[4] if $tag->[0] eq "ALT";
					$result = $x->[5] if $tag->[0] eq "QUAL";
					$result = $x->[6] if $tag->[0] eq "FILTER";
					$result = $x->[7] if $tag->[0] eq "INFO";

					#$result = $x->[-1] if $tag->[0] eq "SAMPLE";
					if ( $tag->[0] eq "SAMPLE" ) {
						if ( $x->[-1] eq "." ) {
							$result = $x->[-2];
						} else {
							$result = $x->[-1];
						}
					}
				}
			}
			$result = "n/a" unless $result;
			$result = "n/a"
			  if $result eq "."
			  or $result eq "''"
			  or $result eq '""';
			$result = "" if $result eq "n/a";
			$str .= "$result;";
		}
		$str =~ s/;+$//;
		$str =~ s/;+/;/g;
		$str =~ s/^;+//;	
		if ($upload){

			my $rowK = $rk.$alts[$i];
			#print "LOADING\t$rowK\t$str\n";
			$hbase->_store($rowK,$str);
			$count++;
			$totalV++;
			if ( $count > 1000 ){
				my $totalT  = time - $time;
				print STDERR "Stored $totalV variants in $totalT seconds " . ($totalV/$totalT) . " variants per second\r";
				$count = 0 ; 
			}
		} else {
			print "$str\n";
		}
	}
}
	print "\n";