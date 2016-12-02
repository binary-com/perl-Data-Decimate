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

use Data::Dumper;

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

subtest "missing_datas" => sub {
    my $data_cache = Data::Resample::DataCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $data_cache, "DataCache instance has been created";

    for (my $i = 0; $i < scalar(@$datas); $i++) {
        $data_cache->data_cache_insert($datas->[$i]);
    }

    my $data = $data_cache->data_cache_get_num_data({
        symbol => 'USDJPY',
        num    => scalar(@$datas),
    });

    is scalar(@$data), '128', "retrieved 128 datas";

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

# try get all resample datas
# last data in our sample
# USDJPY,1479203250,1479203250,108.254,108.256,108.257
    my $resample_data = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_data), '13', "retrieved 13 resample datas";

};

subtest "backfill_with_missing_datas" => sub {
    my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

    my ($raw_key, $resample_key) = map { $resample_cache->_make_key('USDJPY', $_) } (0 .. 1);

    $redis->zremrangebyscore($raw_key,      0, 1479203250);
    $redis->zremrangebyscore($resample_key, 0, 1479203250);

    my $resample_data = $resample_cache->resample_cache_backfill({
        symbol => 'USDJPY',
        data   => $datas,
    });

    is scalar(@$resample_data), '10', "10 resample data";
};

sub datas_from_csv {
    my $filename = 't/sampledata2.csv';

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
