use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Data::Resample',
    AUTHOR           => q{Binary.com <support@binary.com>},
    VERSION_FROM     => 'lib/Data/Resample.pm',
    ABSTRACT_FROM    => 'lib/Data/Resample.pm',
    LICENSE          => 'artistic_2',
    PL_FILES         => {},
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More'         => 0,
        'Test::Deep'         => 0,
        'Test::NoWarnings'   => 0,
        'Test::Exception'    => 0,
        'Moose'              => 0,
        'Moose::Util::TypeConstraints'   => 0,
        'Test::Perl::Critic' => 0,
    },
    PREREQ_PM => {
        'Cache::RedisDB'              => 0,
        'Date::Utility'               => 0,
        'Scalar::Util'                => 0,
        'List::Util'                  => 0,
        'Sereal::Encoder'             => 0,
        'Sereal::Decoder'             => 0,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'Data-Resample-*' },
);
