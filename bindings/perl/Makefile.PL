use 5.008008;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'CircularDB',
    VERSION_FROM      => 'lib/CircularDB.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    #($] >= 5.005 ?     ## Add these new keywords supported since 5.005
    #  (ABSTRACT_FROM  => 'lib/CircularDB.pm', # retrieve abstract from module
    #   AUTHOR         => 'Dan Sully <daniel-github@electricrain.com>') : ()),
    LIBS              => ['-lgsl -lgslcblas -lcirculardb'], # e.g., '-lm'
    DEFINE            => '-Wall -Werror -Wpointer-arith',
    INC               => '-I. -I../../include',
	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
);
