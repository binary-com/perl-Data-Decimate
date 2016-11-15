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

$ENV{REDIS_CACHE_SERVER} = '127.0.0.1:' . $server->port;

ok $server, "test redis server object instance has been created";

my $redis = Cache::RedisDB->redis;

ok $redis, "test redis connection";

done_testing;
