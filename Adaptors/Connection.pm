#!perl
# Prototype hbase connection by Simon White
package Adaptors::Connection;

use strict;
use Bio::EnsEMBL::Utils::Exception qw(throw);
use Data::Dumper;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use Bio::EnsEMBL::Registry;
use Bio::EnsEMBL::Utils::Argument qw(rearrange);
use Hbase::Hbase;

sub new {
	my ( $class, @args ) = @_;
	my $self = bless {}, $class;
	my ( $host, $port, $sendTimeout, $rcrvTimeout,$registry,$species ) =
	  rearrange( [ 'HOST', 'PORT', 'SENDTIMEOUT', 'RCRVTIMEOUT','REGISTRY','SPECIES' ],
				 @args );

	$self->throw("Connection adaptor requires at least a  host and port")
	  unless ( $host && $port) ;
	$self->host($host);
	$self->port($port);
	
	# defaults
	$sendTimeout = 100000 unless $sendTimeout;    # default is for 100 seconds
	$rcrvTimeout = 200000 unless $rcrvTimeout;    # default is 200 seconds
	$self->sendTimeout($sendTimeout);
	$self->rcrvTimeout($rcrvTimeout);
	$species = "human" unless $species;

	# open the db connection
	$self->open();
	
	# get and store adaptors for ensembl objects
	$self->throw("Cannot fetch data without registry file $registry\n")
		unless($registry);
	my $reg;

	$reg = 'Bio::EnsEMBL::Registry';
  	$reg->load_all($registry,undef,1);
	$reg->set_reconnect_when_lost();
	$self->throw("Registry $registry does not appear to be valid\n")
		unless($reg);

	my $aa = $reg->get_adaptor( $species, "core", "analysis" );
	$self->analysisAdaptor($aa);
	my $sa = $reg->get_adaptor( $species, "variation", "individual" );
	$self->sampleAdaptor($sa);
	my $pa = $reg->get_adaptor( $species, "variation", "phenotype" );
	$self->phenotypeAdaptor($pa);
	my $pa = $reg->get_adaptor( $species, "variation", "phenotypefeature" );
	$self->phenotypeFeatureAdaptor($pa);
	$self->registry($reg);
	$self->species($species);
	
	return $self;
}


sub open {
	my ($self) = @_;
	eval {
		my $socket = Thrift::Socket->new( $self->host, $self->port );
		$socket->setSendTimeout( $self->sendTimeout );
		$socket->setRecvTimeout( $self->rcrvTimeout );
		my $transport = Thrift::BufferedTransport->new($socket);
		my $protocol  = Thrift::BinaryProtocol->new($transport);
		my $client    = Hbase::HbaseClient->new($protocol);
		$transport->open();
		$self->transport($transport);
		$self->protocol($protocol);
		$self->client($client);
		print STDERR "Connecting to $client ". $self->host ." " . $self->port ."\n";
	};
	if ($@) {
		print STDERR Dumper $@;
		$self->throw(   "Problem connecting to db "
					  . $self->host
					  . " at port "
					  . $self->port
					  . "\n" );
	}
	return;
}

sub close {
	my ($self) = @_;
	eval { $self->transport->close(); };
	if ($@) {
		print STDERR Dumper $@;
		$self->throw(   "Problem closing connection to db "
					  . $self->host
					  . " at port "
					  . $self->port
					  . "\n" );
	}
}



########### Containers ##################
sub host {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'host'} = $value;
	}
	return $self->{'host'};
}

sub port {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'port'} = $value;
	}
	return $self->{'port'};
}

sub sendTimeout {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'sendTimeout'} = $value;
	}
	return $self->{'sendTimeout'};
}

sub rcrvTimeout {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'rcrvTimeout'} = $value;
	}
	return $self->{'rcrvTimeout'};
}

sub protocol {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'protocol'} = $value;
	}
	return $self->{'protocol'};
}

sub client {
	my ( $self, $value ) = @_;
	if ($value) {
		unless ( $value->isa("Hbase::HbaseClient") ) {
			$self->throw(   "Adaptor type should be Hbase::HbaseClient not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'client'} = $value;
	}
	return $self->{'client'};
}

sub transport {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'transport'} = $value;
	}
	return $self->{'transport'};
}

sub species {
	my ( $self, $value ) = @_;
	if ($value) {
		$self->{'species'} = $value;
	}
	return $self->{'species'};
}

sub analysisAdaptor {
	my ( $self, $value ) = @_;
	if (defined $value) {
		unless ( $value->isa("Bio::EnsEMBL::DBSQL::AnalysisAdaptor") ) {
			$self->throw(   "Adaptor type should be Bio::EnsEMBL::DBSQL::AnalysisAdaptor not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'analysisAdaptor'} = $value;
	}
	return $self->{'analysisAdaptor'};
}

sub sampleAdaptor {
	my ( $self, $value ) = @_;
	if (defined $value) {
		unless ( $value->isa("Bio::EnsEMBL::Variation::DBSQL::IndividualAdaptor") ) {
			$self->throw(   "Adaptor type should be Bio::EnsEMBL::Variation::DBSQL::IndividualAdaptor not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'sampleAdaptor'} = $value;
	}
	return $self->{'sampleAdaptor'};
}

sub phenotypeAdaptor {
	my ( $self, $value ) = @_;
	if (defined $value) {
		unless ( $value->isa("Bio::EnsEMBL::Variation::DBSQL::PhenotypeAdaptor") ) {
			$self->throw(   "Adaptor type should be Bio::EnsEMBL::Variation::DBSQL::PhenotypeAdaptor not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'phenotypeAdaptor'} = $value;
	}
	return $self->{'phenotypeAdaptor'};
}

sub phenotypeFeatureAdaptor {
	my ( $self, $value ) = @_;
	if (defined $value) {
		unless ( $value->isa("Bio::EnsEMBL::Variation::DBSQL::PhenotypeFeatureAdaptor") ) {
			$self->throw(   "Adaptor type should be Bio::EnsEMBL::Variation::DBSQL::PhenotypeFeatureAdaptor not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'phenotypeFeatureAdaptor'} = $value;
	}
	return $self->{'phenotypeFeatureAdaptor'};
}

sub registry {
	my ( $self, $value ) = @_;
	if (defined $value) {
		unless ( $value->isa("Bio::EnsEMBL::Registry") ) {
			$self->throw(   "Adaptor type should be Bio::EnsEMBL::Registry not "
						  . ref($value)
						  . "\n" );
		}
		$self->{'registry'} = $value;
	}
	return $self->{'registry'};
}


1;
