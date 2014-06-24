#!perl
use strict;
use warnings;
use Getopt::Long;
use Env qw(REGISTRY);
$| = 1;
my $file1;
my $file2;
&GetOptions( '1:s' => \$file1,
			 '2:s' => \$file2, );
my $usage = "comparePfiles.pl 
-1 	file1
-2  file2
";
die $usage unless $file1 && $file2;
open( my $one, $file1 ) or die("Cannot open file $file1 for reading\n");
open( my $two, $file2 ) or die("Cannot open file $file2 for reading\n");

# sync them so we read line at a time from both
my $fh = $one;
my ($p1,$p2) = 0;
my ($d1,$d2) = 0;
while (<$fh>) {
	chomp;

	my ($chr,$pos, $depth)= split("\t");
	#print "FH $fh $chr $pos $depth\n";
	if ( $fh == $one){
		$p1 = $pos;
		$d1 = $depth;
		if ($pos > $p2){
			$fh = switch($fh);
			# advance
			next;
		}

	} else {
		if ( $pos > $p1){
			$p2 = $pos;
			$d2 = $depth;
			$fh = switch($fh);
			# advance
			next;
		}
	}
	if ( $p1 == $p2 ){

		if ( $d1 == $d2 ){
			#print "DEPTHS MATCH TOO $d1 $d2\n";
		} else {
			print "MATCH $p1 = $p2 DISASTER STRIKES!!!! $d1 != $d2\n";
		}
	}
	if ( $d1 or $d2 ){
		# we should have positions for both if a depth is defined
		unless ($p1 && $p2){
			print "MISSING $p1 ? $p2 DISASTER STRIKES!!!! $d1  $d2\n";
		}
	}

}


sub switch {
	my ($fh) = @_;
		#swap files
	if ( $fh == $one){
		$fh = $two;
	} else {
		$fh = $one;
	}
	return $fh;
}