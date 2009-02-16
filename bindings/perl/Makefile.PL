use 5.008008;
use ExtUtils::MakeMaker;


WriteMakefile(
    NAME              => 'CircularDB',
    VERSION_FROM      => 'lib/CircularDB.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    #($] >= 5.005 ?     ## Add these new keywords supported since 5.005
    #  (ABSTRACT_FROM  => 'lib/CircularDB.pm', # retrieve abstract from module
    #   AUTHOR         => 'Dan Sully <daniel-github@electricrain.com>') : ()),
    LIBS              => ['-L/opt/local/lib -lgsl -lgslcblas'], # e.g., '-lm'
    #LIBS              => ['-L/opt/local/lib -lgsl -lgslcblas -lcirculardb'], # e.g., '-lm'
    #DEFINE            => '-std=c99 -Wall -Werror -Wpointer-arith -Wwrite-strings', # e.g., '-DHAVE_SOMETHING'
    DEFINE            => '-O2 -Wall', # e.g., '-DHAVE_SOMETHING'
    INC               => '-I. -I../../include -I/opt/local/include', # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
);
