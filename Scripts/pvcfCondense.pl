#!perl
use vars qw(%Config);
use strict;
use Getopt::Long;

# reads a .pvcf and imports into the db
my $out;
my $file;
my $meta;
$| = 1;
my $usage = "pvcfTextImport.pl
  -file AVCF file to be parsed
  -meta	file of sample names and associated meta data cohort - gender etc
  -out  where to write the files
";
my $file1;
&GetOptions( 'file:s' => \$file,
			 'out:s'  => \$out,
			 'meta:s' => \$meta );
die($usage) unless ( $out );

# parse meta file
my $cohort;
my $gender;
my @cohorts;
my @cols = ("0/0","0/1","1/1","No_Data","Fail");
my $cohortOrder;

# read the meta data
open ( META,$meta ) or die("Cannot open meta data file for parsing\n");
while (<META>){
	chomp;
	if ( $_ =~ /(\S+)\t(\S+)\t(\S+)/ ) {
		$cohort->{$1} = $2;
		$gender->{$1} = $3;
		$cohortOrder->{$2}++;
	} else {
		die("Cannot parse meta file in format $_ - should be sample\tcohort\tgender\n");
	}
}


my $fh = \*STDIN;
open( DATA, ">$out/data.txt" );
my @header;
# hash for keeping track of entries
my %entry;
my $count = 0;

# print the header line
print "Chrom\tPos\tRef\tAlt";
foreach my $c ( sort keys %{$cohortOrder} ){
	foreach my $col ( @cols ){
		print "\t$c male $col";
	}
	foreach my $col ( @cols ){
		print "\t$c female $col";
	}								
}
print "\n";

while (<$fh>) {
	chomp;
	# hash each row
	my $hash;
	my %FORMAT;
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
	my $fmtStr = $x[8];
	my @fmtArr = split(":",$fmtStr);
	# map the format tags to array positions
	for ( my $i = 0 ; $i < scalar(@fmtArr); $i++){
		$FORMAT{$fmtArr[$i]} = $i;
	}
	
	for ( my $i = 9 ; $i < scalar(@x) ; $i++){
		# hash the row
		my $c = $cohort->{$header[$i]};
		my $g = $gender->{$header[$i]};
		die("Missing meta data for sample " . $header[$i] . " cohort - $c and gender $g\n" ) unless ( $c && $g);
		# build the hash
		#print $x[$i] ."\n";
		my $ref  = $x[3];
		my $gt   = getVal("GT",\%FORMAT,$x[$i]);
		my $alt  = getVal("AA",\%FORMAT,$x[$i]);	
		my $filt = getVal("FT",\%FORMAT,$x[$i]);	
		if ($filt =~ /PASS/ or $filt eq 'No_var' or $filt eq 'high_coverage'){
			# count the genotype
			# hash is ref - alt - cohort - gender - - genotype = count 
			$hash->{$ref}->{$alt}->{$c}->{$g}->{$gt}++;
		} elsif ($filt eq 'No_data'){
			$hash->{$ref}->{$alt}->{$c}->{$g}->{'No_Data'}++;
		} else {
			$hash->{$ref}->{$alt}->{$c}->{$g}->{'Fail'}++;
		}
	}
	my $cnt = 0;
	# now we print out the row
	foreach my $ref ( keys %$hash ){
		foreach my $alt ( keys %{$hash->{$ref}} ){
			print $x[0]."\t".$x[1]."\t$ref\t$alt";
			# now print out the data in order
			foreach my $c ( sort keys %{$cohortOrder} ){
				foreach my $col ( @cols ){
					my $num = 0;
					$num += $hash->{$ref}->{$alt}->{$c}->{'male'}->{$col};
					print "\t$num";
					$cnt += $num;
				}
				foreach my $col ( @cols ){
					my $num = 0;
					$num += $hash->{$ref}->{$alt}->{$c}->{'female'}->{$col};
					print "\t$num";
					$cnt += $num;
				}								
			}
			print "\n";
		}
	}
	# sanity check - total of numbers per row shuold equal the number of samples
	die("Number of genotypes ( $cnt ) not equal to the number of samples ( ". scalar(keys %{$cohort})." ) \n")
		unless ( scalar(keys %{$cohort}) == $cnt );

}

sub getVal {
	my ($val,$fmt,$str) = @_;
	my @arr = split(":",$str);
	my $data = $arr[$fmt->{$val}];
	die("Cannot figure out how to get $val from $str \n")
		unless $data;
	return $data;
}