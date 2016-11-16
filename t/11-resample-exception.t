use strict;
use warnings;

use Date::Utility;
use RedisDB;
use Path::Tiny;
use Test::More;
use Test::RedisServer;
use Test::TCP;
use Test::FailWarnings;
use Text::CSV;

use Data::Resample::TicksCache;
use Data::Resample::ResampleCache;

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

ok $server, "test redis server object instance has been created";

my $redis = RedisDB->new(
    host => 'localhost',
    port => $server->port
);

ok $redis, "redisdb object instance has been created";

subtest "ticks_cache_exception" => sub {
    my $ticks_cache = Data::Resample::TicksCache->new({
        redis => $redis,
    });

    ok $ticks_cache, "TicksCache instance has been created";

};

subtest "resample_cache_exception" => sub {

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

};

done_testing;
