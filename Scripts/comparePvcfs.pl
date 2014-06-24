#!perl

use strict;
use warnings;


use Getopt::Long;
use Env qw(REGISTRY);
$| = 1;

my $vcf1;
my $vcf2;

&GetOptions(
	    '1:s'  	=> \$vcf1,
	    '2:s'	=> \$vcf2,
	    	   );
	
my $usage = "comparePvcfs.pl 
-1 	vcf1
-1  vcf2
";	   
die $usage unless $vcf1 && $vcf2;

open ( my $one ,$vcf1 ) or die("Cannot open file $vcf1 for reading\n");
open ( my $two ,$vcf2 ) or die("Cannot open file $vcf2 for reading\n");

# sync them so we read line at a time from both
my $fh = $one;

my $data1;
my $data2;
my @header1;
my @header2;
my $exceptions;
my $correct;
my $trend;
my $count = 0;

while (<$one> ){
	chomp;
	if ($_ =~/^#/){
		@header1 = split("\t") unless @header1;
		@header2 = split("\t") unless @header2;
		next;
	}
	my @line = split("\t");
	for ( my $i = 9 ; $i < scalar(@line); $i++){
		$data1->{$line[1]}->{$header1[$i]} = $line[$i];
	}
}
while (<$two> ){
	chomp;
	if ($_ =~/^#/){
		@header1 = split("\t") unless @header1;
		@header2 = split("\t") unless @header2;
		next;
	}
	my @line = split("\t");

	for ( my $i = 9 ; $i < scalar(@line); $i++){
		$data2->{$line[1]}->{$header2[$i]} = $line[$i];
	}
}
compare($data1,$data2);

print STDERR "Finished\n";

print STDERR "\n=-=-=-=-=-=-=-==-=-=-=-=-=-=-=--=--=\n\n$count variants compared.\nFAIL Summary:\n";
foreach my $key ( keys %$exceptions ){
	my $num = sprintf("%.2f",($exceptions->{$key} / $count)*100);
	print STDERR "$key:\t". $exceptions->{$key} . "\t$num%\n";
}
print STDERR "\nTrends Summary:\n";
foreach my $key ( keys %$trend ){
	my $num = sprintf("%.2f",($trend->{$key} / $count)*100);
	print STDERR "$key:\t". $trend->{$key} . "\t$num%\n";
}
print STDERR "\nPASS Summary:\n";
foreach my $key ( keys %$correct ){
	my $num = sprintf("%.2f",($correct->{$key} / $count)*100);
	print STDERR "$key:\t". $correct->{$key} . "\t$num%\n";
}

exit;

sub compare {
	my ($doo1,$doo2)=@_;
	foreach my $pos ( keys %$doo1 ){
	my $d1 = $doo1->{$pos};
	my $d2 = $doo2->{$pos};
	$exceptions->{"missing line"}++ unless $d2;
	next unless $d2;
	#print "Got " . scalar(keys %{$d1}) . " keys \n";
	foreach my $key ( keys %{$d1}){
		$count++;
		my @a1 = split(":",$d1->{$key} );
		my @a2 = split(":",$d2->{$key} );
		if ($d1->{$key} eq $d2->{$key}){
			#print "\t$key:SAME";
			$trend->{"perfect match"}++;
			$correct->{$a1[6] }++;
		} 
		else {
			# list of exceptions
			# 1. GQ0 
			if ( $a1[6] eq "No_var" && $a2[5] eq "0"){
					$trend->{"No_var GQ = 0"}++;
			}
			if ( $a1[6] eq "No_var" && $a2[5] eq "."){
					$trend->{"No_var GQ = ."}++;
			}
			if ( $a1[6] eq "No_var"){
				$trend->{"total No_var "}++;
			}
			
			# 1. The are both no var but they have more reads than we put
			if ( $a1[6] eq $a2[6] && $a1[6] eq "No_var"){
				if ( $a1[3] < $a2[3]){
					$exceptions->{"No_var different depths"}++;
					next;
				}
			}
			# 2. the no var is less than 6 
			if ( $a1[6] eq "No_data"){
				if ( $a2[3] < 6 ){
					$exceptions->{"No_data depth < 6"}++;
					next;
				}
			}
			# 3. I say no var they say low var ratio
			if ( $a1[6] eq "No_var" && $a2[6] =~ "low_VariantRatio"){
				if ( $a2[3] >= 6 ){
					$exceptions->{"No_var low VR"}++;
					next;
				}
			}
			# 3. I say no var they say low var ratio
			if ( $a1[6] eq "PASS" && $a2[6] =~ "high_coverage"){
				if ( $a2[3] >= 6 ){
					$exceptions->{"PASS High cov"}++;
					next;
				}
			}
			# 3. I say No_var they have  depth lower than 6
			if ( $a1[6] eq "No_var" ){
				if ( $a2[3] < 6 ){
					$exceptions->{"No_var depth < 6"}++;
				}
			}
			
			if ( $a1[6] eq "No_var" && $a2[6] =~ /low_snpqual/){
					$exceptions->{"No_var low_snp_qual"}++;
					next;
			}
			if (  $a2[6] =~ /high_coverage/){
					$exceptions->{"high_coverage"}++;
					next;
			}
					
			#5. store some random others
			my $thing1 = $a1[6];
			my $thing2 = $a2[6];
			# just call it low if it a low thing
			$thing1 = 'low' if $thing1 =~ /low/;
			$thing2 = 'low' if $thing2 =~ /low/;
			$exceptions->{"$thing1 - $thing2"}++;
			print "\n$pos\t$key:DIFF \t$thing1 - $thing2\t" . $d1->{$key} . "\t". $d2->{$key} ;
		}
	}
	}
	
}
	   
