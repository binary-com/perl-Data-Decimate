use strict;
use warnings;

use Date::Utility;
use Cache::RedisDB;
use Path::Tiny;
use Test::More;
use Test::RedisServer;
use Test::TCP;
use Test::FailWarnings;

use Data::Resample::TicksCache;

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

$ENV{REDIS_CACHE_SERVER} = '127.0.0.1:' . $server->port;

ok $server, "test redis server object instance has been created";

my $redis = Cache::RedisDB->redis;

ok $redis, "test redis connection";

subtest "ticks_cache_insert" => sub {
    my $ticks_cache = Data::Resample::TicksCache->new;

    ok $ticks_cache, "TicksCache instance has been created";

    my %tick = (
        symbol => 'USDJPY',
        epoch  => time,
        quote  => 103.0,
        bid    => 103.0,
        ask    => 103.0,
    );

    $ticks_cache->tick_cache_insert({
        tick   => \%tick,
    });

};

done_testing;
