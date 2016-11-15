use strict;
use warnings;

use Date::Utility;
use RedisDB;
use Path::Tiny;
use Test::More;
use Test::RedisServer;
use Test::TCP;
use Test::FailWarnings;

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

subtest "ticks_cache_insert_and_retrieve" => sub {
    my $ticks_cache = Data::Resample::TicksCache->new({
        redis => $redis,
    });

    ok $ticks_cache, "TicksCache instance has been created";

    my %tick = (
        symbol => 'USDJPY',
        epoch  => time,
        quote  => 103.0,
        bid    => 103.0,
        ask    => 103.0,
    );

    $ticks_cache->tick_cache_insert(\%tick);

    my $ticks = $ticks_cache->tick_cache_get_num_ticks({
        symbol => 'USDJPY',
    });

    is scalar(@$ticks), '1', "retrieved one tick";

};

subtest "resample_cache" => sub {
    my $resample_cache = Data::Resample::ResampleCache->new({
        redis => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";
};

done_testing;