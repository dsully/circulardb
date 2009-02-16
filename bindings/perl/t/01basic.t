# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 54;
use File::Temp qw(tempfile);

use CircularDB;
#use Data::Dump qw(dump);
use Fcntl qw(:DEFAULT);

my ($file, $name);
my $flags = 'rw';

sub setup {
    my $fh;
    ($fh, $file) = tempfile("tempXXXXX", SUFFIX => ".cdb", DIR => "/tmp");

    $name = "Testing Ruby CDB";

    # We just want the filename.
    close($fh);
    unlink($file);
}

sub teardown {
    unlink($file);
}

sub test_create {
    setup();

    my $cdb = CircularDB::Storage->new($file, $flags, undef, $name);

    $cdb->max_value(100);
    ok($cdb);
    ok($cdb->filename eq $file);
    ok($cdb->name eq $name);
    ok($cdb->type eq "gauge");
    ok($cdb->units eq "absolute");
    ok($cdb->min_value == 0);
    ok($cdb->max_value == 100);
    $cdb->close;

    teardown()
}

sub test_rw {
    setup();

    my $records = [];
    my $now     = scalar(time());

    my $flags       = 0; #File::CREAT|File::RDWR|File::EXCL;
    my $mode        = undef;
    my $max_records = undef;
    my $type        = "gauge";
    my $units       = "absolute";
    my $min_value   = 0;
    my $max_value   = 0;
    my $interval    = 300;

    for (my $i = 1; $i < 11; $i++) {
      push @$records, [ $now, $i ];
      $now += 1
    }

    my $cdb = CircularDB::Storage->new($file, $flags, $mode, $name, $max_records, $type, $units, $min_value, $max_value, $interval);
    ok($cdb);
    ok($cdb->max_value == $max_value);

    ok(10 == $cdb->write_records($records));

    my $read = $cdb->read_records;

    ok(10 == scalar(@$read));
    ok(10 == $cdb->num_records);

    for (my $i = 0; $i < @$records; $i++) {
      ok($records->[$i][0] == $read->[$i][0]);
      ok($records->[$i][1] == $read->[$i][1]);
    }

    #ok(5.5 == $cdb->statistics->mean);
    #ok(5.5 == $cdb->statistics->median);
    #ok(55 == $cdb->statistics->sum);
    #ok(10 == $cdb->statistics->max);
    #ok(1 == $cdb->statistics->min);

    # Check setting a float and reading it back
    ok(1 == $cdb->update_record($records->[5][0], 999.0005));

    my $float_check = $cdb->read_records;

    ok($float_check->[5][1] == 999.0005);

    # Check setting value to undef
    ok(1 == $cdb->update_record($records->[6][0], undef));

    my $undef_check = $cdb->read_records;

    ok(!defined $undef_check->[6][1]);

    $cdb->close;

    teardown()
}

sub test_counter_reset {
    setup();

    my $cdb = CircularDB::Storage->new($file, $flags, undef, $name, undef, "counter");

    ok($cdb);

    my $records = [];
    my $now     = scalar(time());

    push @$records, [ $now+0, 10 ];
    push @$records, [ $now+1, 11 ];
    push @$records, [ $now+2, 12 ];
    push @$records, [ $now+3, 0 ];
    push @$records, [ $now+4, 1 ];

    ok(5 == $cdb->write_records($records));

    my $read = $cdb->read_records;

    ok(10 == $read->[0][1]);
    ok(1 == $read->[1][1]);
    ok(1 == $read->[2][1]);
    ok(!defined $read->[3][1]);
    ok(1 == $read->[4][1]);

    $cdb->close;

    teardown();
}

sub test_step {
    setup();

    my $cdb = CircularDB::Storage->new($file, $flags, undef, $name, undef, "gauge");
    ok($cdb);

    my $records = [];
    my $start   = 1190860358;

    for (my $i = 0; $i < 20; $i++) {
        push @$records, [$start+$i, $i];
    }

    ok(20 == $cdb->write_records($records));

    my $read = $cdb->read_records(0, 0, 0, 1, 5);

    # for first 5, should have time of 1190860360 and value of 2
    # for next  5, should have time of 1190860365 and value of 7
    ok(4 == scalar(@$read));

    ok(1190860360 == $read->[0][0]);
    ok(2 == $read->[0][1]);

    ok(1190860365 == $read->[1][0]);
    ok(7 == $read->[1][1]);

    ok(1190860370 == $read->[2][0]);
    ok(12 == $read->[2][1]);

    ok(1190860375 == $read->[3][0]);
    ok(17 == $read->[3][1]);

    $cdb->close;

    teardown();
}

test_create();
test_rw();
test_counter_reset();
test_step();

# vim: set tabstop=4 expandtab shiftwidth=4:
