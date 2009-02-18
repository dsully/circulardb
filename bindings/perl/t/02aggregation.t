# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 17;
use File::Temp qw(tempdir);
use File::Path;

use CircularDB;
use Fcntl qw(:DEFAULT);

my $name = "Testing Aggregate CDB";
my $flags = O_RDWR|O_CREAT|O_EXCL;
my $tmpdir;

sub setup {
    $tmpdir = tempdir();
}

sub teardown {
    rmtree($tmpdir);
}

sub test_aggregate {
    setup();

    my @times = ();
    my $now   = scalar(time);

    for (my $i = 1; $i < 11; $i++) {
        push @times, ($now + $i);
    }

    my $agg = CircularDB::Aggregate->new("test");
    ok($agg);

    for (my $i = 1; $i < 4; $i++) {

        my @records = ();

        for ($j = 1; $j < 11; $j++) {
            push @records, [ $times[$j-1], $j ];
        }

        my $cdb = CircularDB::Storage->new(join('/', $tmpdir, "$i.cdb"), $flags, undef, $name);
        $cdb->write_records(\@records);
        $agg->add_cdb($cdb);
    }

    my $read = $agg->read_records;

    ok(10 == scalar(@$read));
    ok(10 == $agg->num_records);

    for (my $i = 1; $i < 10; $i++) {
      ok(($i+1)*3 == $read->[$i][1]);
    }

    ok(165  == $agg->statistics->sum);
    ok(3.0  == $agg->statistics->min);
    ok(30.0 == $agg->statistics->max);
    ok(16.5 == $agg->statistics->median);
    ok(16.5 == $agg->statistics->mean);

    $agg->close;
}

test_aggregate();

# vim: set tabstop=4 expandtab shiftwidth=4:
