#!perl

use strict;
use warnings;
use Data::Dumper;
use Thrift::API::HiveClient2;

    my $client = Thrift::API::HiveClient2->new(
        host    => "hadoop-node1",
        port    => 10000,
    );

     $client->connect() or die "Failed to connect";

print "Got connecttion YAY\n";

my $rh = $client->execute("CREATE TABLE page_view(viewTime INT, userid BIGINT,
                page_url STRING, referrer_url STRING,
                ip STRING COMMENT 'IP Address of the User')
COMMENT 'This is the page view table'
PARTITIONED BY(dt STRING, country STRING)
STORED AS SEQUENCEFILE;");

$client->execute("SHOW TABLES;")