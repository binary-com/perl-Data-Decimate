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

use Data::Resample::DataCache;
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

my $datas = datas_from_csv();

subtest "data_cache_insert_and_retrieve" => sub {
    my $data_cache = Data::Resample::DataCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $data_cache, "DataCache instance has been created";

    my $first_data = $datas->[0];

    $data_cache->data_cache_insert($first_data);

    my $data = $data_cache->data_cache_get_num_data({
        symbol => 'USDJPY',
    });

    is scalar(@$data), '1', "retrieved one data";

#USDJPY,1479203101,1479203115,108.26,108.263,108.265
    is $data->[0]->{epoch}, '1479203101', "epoch is correct";

#test for data insertion that cross 15s boundary
    for (my $i = 1; $i <= 16; $i++) {
        $data_cache->data_cache_insert($datas->[$i]);
    }

    my $data2 = $data_cache->data_cache_get_num_data({
        symbol => 'USDJPY',
        num    => 17,
    });

    is scalar(@$data2), '17', "retrieved 17 datas";

#now, try to retrieve our first resample data
#last inserted data
#USDJPY,1479203118,1479203130,108.29,108.291,108.291
    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    my $resample_data = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203118,
    });

    is scalar(@$resample_data), '1', "retrieved 1 resample data";

#now let's try insert the rest of the data
    for (my $i = 17; $i <= 141; $i++) {
        $data_cache->data_cache_insert($datas->[$i]);
    }

    my $data3 = $data_cache->data_cache_get_num_data({
        symbol => 'USDJPY',
        num    => 142,
    });

    is scalar(@$data3), '142', "retrieved 142 datas";

    $data3 = $data_cache->data_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203127,
    });

    is scalar(@$data3), '25', "retrieved 25 datas";

# try get all resample datas
# last data in our sample
# USDJPY,1479203250,1479203250,108.254,108.256,108.257
    $resample_data = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_data), '17', "retrieved 17 resample datas";
};

subtest "backfill_test" => sub {

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

    my ($raw_key, $resample_key) = map { $resample_cache->_make_key('USDJPY', $_) } (0 .. 1);

    $redis->zremrangebyscore($raw_key, 0, 1479203250);
    $redis->zremrangebyscore($resample_key,   0, 1479203250);

    $resample_cache->resample_cache_backfill({
        symbol => 'USDJPY',
        data   => $datas,
    });

    my $resample_data = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_data), '10', "retrieved 10 resample datas";
};

subtest "backtest_mode" => sub {

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

    my ($raw_key, $resample_key) = map { $resample_cache->_make_key('USDJPY', $_) } (0 .. 1);

    $redis->zremrangebyscore($raw_key, 0, 1479203250);
    $redis->zremrangebyscore($resample_key,   0, 1479203250);

    my $resample_datas = $resample_cache->resample_cache_backfill({
        symbol   => 'USDJPY',
        data     => $datas,
        backtest => 1,
    });

    is scalar(@$resample_datas), '10', "10 resample data";

    my $resample_data = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_data), '0', "retrieved 0 resample data. for backtest mode, data will not be saved into redis.";
};

sub datas_from_csv {
    my $filename = 't/sampledata.csv';

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

    my @datas;

# parse the rest
    while (my $row = $csv->getline_hr($fh)) {
        push @datas, $row;
    }

    $csv->eof or $csv->error_diag;
    close $fh;

    return \@datas;
}

done_testing;
