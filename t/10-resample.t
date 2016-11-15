use strict;
use warnings;

use Date::Utility;
use Cache::RedisDB;
use Path::Tiny;
use Test::More;
use Test::RedisServer;
use Test::TCP;
use Test::FailWarnings;

my $tmp_dir = Path::Tiny->tempdir(CLEANUP => 1);

my $server = Test::TCP->new(
    code => sub {
        my $port = shift;
        Test::RedisServer->new(
            auto_start => 1,
            conf       => {port => $port},
            tmpdir     => $tmp_dir,
        )->exec;
    });
