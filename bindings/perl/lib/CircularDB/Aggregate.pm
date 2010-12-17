package CircularDB::Aggregate;

# TODO: Backing store.

use base qw(Class::Accessor);

__PACKAGE__->mk_accessors(qw(name type units));

sub add_cdb {
  my ($self, $cdb) = @_;
  return $self->add_cdbs([$cdb]);
}

sub add_cdbs {
  my ($self, $cdbs) = @_;

  if (scalar(@$cdbs) > 0) {

    # Type & Units need to be the same.
    $self->type($cdbs->[0]->type)   if !$self->type;
    $self->units($cdbs->[0]->units) if !$self->units;

    for my $cdb (@$cdbs) {

      if ($cdb->type ne $self->type) {
        warn sprintf("CircularDB type must be the same for aggregation: %s != %s : %s\n", $cdb->type, $self->type, $cdb->filename);
        return;
      }

      if ($cdb->units ne $self->units) {
        warn sprintf("CircularDB units must be the same for aggregation: %s != %s : %s\n", $cdb->units, $self->units, $cdb->filename);
        return;
      }
    }

    # Largest number of records is the driver.
    push @{$self->{'cdbs'}}, (sort { $b->num_records <=> $a->num_records } @$cdbs);
  }

  return $self->{'cdbs'};
}

sub cdbs {
  my $self = shift;
  return $self->{'cdbs'};
}

sub filename {
  my $self = shift;
  return $self->{'cdbs'}->[0]->filename;
}

sub driver_start_time {
  my $self = shift;
  #return $self->{'cdbs'}->[0]->read_records(0, 0, -1)[0][0];
}

sub driver_end_time {
  my $self = shift;
  #return $self->{'cdbs'}->[0]->read_records(0, 0, -1)[-1][0];
}

sub min_value {
  my $self = shift;
  return $self->{'cdbs'}->[0]->min_value;
}

sub max_value {
  my $self = shift;
  return $self->{'cdbs'}->[0]->max_value;
}

sub num_records {
  my $self = shift;
  return $self->{'cdbs'}->[0]->num_records;
}

sub last_updated {
  my $self = shift;
  return $self->{'cdbs'}->[0]->last_updated;
}

sub statistics {
  my ($self) = @_;

  if (!defined $self->{'statistics'}) {
    $self->read_records;
  }

  return $self->{'statistics'};
}

sub size {
  # Only aggregate percentage sizes by division, otherwise we want the
  # total - ie: Network traffic.
  if ($self->{'units'} =~ /transmitted|recieved/) {
    return 1.0;
  } else {
    return scalar @{$self->{'cdbs'}};
  }
}

sub close {
  my ($self) = @_;

  for my $cdb (@{$self->{'cdbs'}}) {
    $cdb->close;
  }
}

1;

__END__
