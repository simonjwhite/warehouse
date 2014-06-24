#!perl
use vars qw(%Config);
use strict;
use Getopt::Long;

# reads a .pvcf and imports into the db
my $out;
my $file;
$| = 1;
my $usage = "pvcfTextImport.pl
  -file AVCF file to be parsed
  -out  where to write the files
";
my $file1;
&GetOptions( 'file:s' => \$file,
			 'out:s'  => \$out, );
die($usage) unless ( $out );

my $fh = \*STDIN;
open( DATA, ">$out/data.txt" );
my @header;
# hash for keeping track of entries
my %entry;
my $count = 0;
while (<$fh>) {
	chomp;
	next if $_ =~ /^##/;
	next if $_ =~ /^----/;
	my @x = split( /\t/, $_ );
	if ($_ =~ /^#CHROM/){
		foreach my $h (@x){
			push @header,$h;
		}
		next;
	} 
	# parse main body
	# row key is chr:pos:ref:alt:META/sampleid"
	my $row_id  =  $x[0] . ":" . $x[1] .":" . $x[3] .":" .$x[4] .":";

	for ( my $i = 2 ; $i < 9 ; $i++){
		# print the file
		print DATA "$row_id:META\t" .$header[$i]."\t". $x[$i] ."\n";
	}
	
	for ( my $i = 9 ; $i < scalar(@x) ; $i++){
		# print the file
		print DATA "$row_id:".$header[$i]."\tFORMATVALUES\t". $x[$i] ."\n";
	}
}