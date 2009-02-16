package CircularDB;

use 5.008008;
use strict;
use warnings;

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('CircularDB', $VERSION);

package CircularDB::Storage;

use base qw(Class::Accessor);

__PACKAGE__->mk_accessors(qw(
    name filename max_records min_value max_value interval units type num_records
));

sub get {
    my ($self, $name) = @_;
    return $self->{'header'}->{$name};
}

sub set {
    my ($self, $name, $value) = @_;

    if (defined $value and $name !~ /^(?:filename|num_records)$/) {
        $self->_set_header($name, $value);
    }

    return $self->{'header'}->{$name};
}

sub last_updated {
    my ($self) = @_;
    return (stat($self->filename))[9];
}

sub size {
    return 1.0;
}

1;

__END__
