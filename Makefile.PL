use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Data::Decimate',
    AUTHOR           => q{Binary.com <support@binary.com>},
    VERSION_FROM     => 'lib/Data/Decimate.pm',
    ABSTRACT_FROM    => 'lib/Data/Decimate.pm',
    LICENSE          => 'artistic_2',
    PL_FILES         => {},
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    TEST_REQUIRES => {
        'Test::More'         => 0,
        'Test::Deep'         => 0,
        'Test::NoWarnings'   => 0,
        'Test::Exception'    => 0,
        'Moose'              => 0,
        'Moose::Util::TypeConstraints'   => 0,
        'Test::Perl::Critic' => 0,
        'Text::CSV'          => 0,
        'Path::Tiny'         => 0,
    },
    PREREQ_PM => {
        'Scalar::Util'                => 0,
        'List::Util'                  => 0,
        'Time::Duration::Concise'     => 0,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'Data-Decimate-*' },
);
