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

my $ticks = ticks_from_csv();

subtest "ticks_cache_insert_and_retrieve" => sub {
    my $ticks_cache = Data::Resample::TicksCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $ticks_cache, "TicksCache instance has been created";

    my $first_tick = $ticks->[0];

    $ticks_cache->tick_cache_insert($first_tick);

    my $tick = $ticks_cache->tick_cache_get_num_ticks({
        symbol => 'USDJPY',
    });

    is scalar(@$tick), '1', "retrieved one tick";

#USDJPY,1479203101,1479203115,108.26,108.263,108.265
    is $tick->[0]->{epoch}, '1479203101', "epoch is correct";

#test for ticks insertion that cross 15s boundary
    for (my $i = 1; $i <= 16; $i++) {
        $ticks_cache->tick_cache_insert($ticks->[$i]);
    }

    my $tick2 = $ticks_cache->tick_cache_get_num_ticks({
        symbol => 'USDJPY',
        num    => 17,
    });

    is scalar(@$tick2), '17', "retrieved 17 ticks";

#now, try to retrieve our first resample tick
#last inserted tick
#USDJPY,1479203118,1479203130,108.29,108.291,108.291
    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    my $resample_tick = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203118,
    });

    is scalar(@$resample_tick), '1', "retrieved 1 resample tick";

#now let's try insert the rest of the ticks data
    for (my $i = 17; $i <= 141; $i++) {
        $ticks_cache->tick_cache_insert($ticks->[$i]);
    }

    my $tick3 = $ticks_cache->tick_cache_get_num_ticks({
        symbol => 'USDJPY',
        num    => 142,
    });

    is scalar(@$tick3), '142', "retrieved 142 ticks";

    $tick3 = $ticks_cache->tick_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203127,
    });

    is scalar(@$tick3), '25', "retrieved 25 ticks";

# try get all resample ticks
# last tick in our sample
# USDJPY,1479203250,1479203250,108.254,108.256,108.257
    $resample_tick = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_tick), '17', "retrieved 17 resample ticks";
};

subtest "backfill_test" => sub {

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

    my ($unagg_key, $agg_key) = map { $resample_cache->_make_key('USDJPY', $_) } (0 .. 1);

    $redis->zremrangebyscore($unagg_key, 0, 1479203250);
    $redis->zremrangebyscore($agg_key,   0, 1479203250);

    $resample_cache->resample_cache_backfill({
        symbol => 'USDJPY',
        ticks  => $ticks,
    });

    my $resample_tick = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_tick), '10', "retrieved 10 resample ticks";
};

subtest "backtest_mode" => sub {

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

    my ($unagg_key, $agg_key) = map { $resample_cache->_make_key('USDJPY', $_) } (0 .. 1);

    $redis->zremrangebyscore($unagg_key, 0, 1479203250);
    $redis->zremrangebyscore($agg_key,   0, 1479203250);

    my $resample_data = $resample_cache->resample_cache_backfill({
        symbol   => 'USDJPY',
        ticks    => $ticks,
        backtest => 1,
    });

    is scalar(@$resample_data), '10', "10 resample data";

    my $resample_tick = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_tick), '0', "retrieved 0 resample ticks. for backtest mode, data will not be saved into redis.";
};

sub ticks_from_csv {
    my $filename = 't/tickdata.csv';

    open(my $fh, '<:utf8', $filename) or die "Can't open $filename: $!";

# skip to the header
    my $header = '';
    while (<$fh>) {
        if (/^symbol,/x) {
            $header = $_;
            last;
        }
    }

    my $csv = Text::CSV->new or die "Text::CSV error: " . Text::CSV->error_diag;

# define column names
    $csv->parse($header);
    $csv->column_names([$csv->fields]);

    my @ticks;

# parse the rest
    while (my $row = $csv->getline_hr($fh)) {
        push @ticks, $row;
    }

    $csv->eof or $csv->error_diag;
    close $fh;

    return \@ticks;
}

done_testing;
