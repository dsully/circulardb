package CircularDB;

use 5.008008;
use strict;
use warnings;

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('CircularDB', $VERSION);

require CircularDB::Storage;
require CircularDB::Aggregate;

1;

__END__
