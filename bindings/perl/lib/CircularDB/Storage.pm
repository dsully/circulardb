package CircularDB::Storage;

use base qw(Class::Accessor);

__PACKAGE__->mk_accessors(qw(
  name filename max_records min_value max_value units type num_records
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

# This is much easier to do here than in XS/C
sub statistics {
  my ($self) = @_;

  if (!defined $self->{'statistics'}) {
    $self->read_records;
  }

  return $self->{'statistics'};
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
