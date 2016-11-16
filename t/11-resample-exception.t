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

my $ticks = ticks_from_csv();

subtest "missing_ticks" => sub {
    my $ticks_cache = Data::Resample::TicksCache->new({
        redis => $redis,
    });

    ok $ticks_cache, "TicksCache instance has been created";

    for (my $i = 0; $i < scalar(@$ticks); $i++) {
        $ticks_cache->tick_cache_insert($ticks->[$i]);
    }

    my $tick = $ticks_cache->tick_cache_get_num_ticks({
        symbol => 'USDJPY',
        num    => scalar(@$ticks),
    });

    is scalar(@$tick), '128', "retrieved 128 ticks";

    my $resample_cache = Data::Resample::ResampleCache->new({
        redis => $redis,
    });

# try get all resample ticks
# last tick in our sample
# USDJPY,1479203250,1479203250,108.254,108.256,108.257
    my $resample_tick = $resample_cache->resample_cache_get({
        symbol      => 'USDJPY',
        start_epoch => 1479203101,
        end_epoch   => 1479203250,
    });

    is scalar(@$resample_tick), '9', "retrieved 9 resample ticks";

};

subtest "backfill_with_missing_ticks" => sub {
    my $resample_cache = Data::Resample::ResampleCache->new({
        redis => $redis,
    });

    ok $resample_cache, "ResampleCache instance has been created";

    my ($unagg_key, $agg_key) = map { $resample_cache->_make_key('USDJPY', $_) } (0 .. 1);

    $redis->zremrangebyscore($unagg_key, 0, 1479203250);
    $redis->zremrangebyscore($agg_key,   0, 1479203250);

    my $resample_data = $resample_cache->resample_cache_backfill({
        symbol => 'USDJPY',
        ticks  => $ticks,
    });

    is scalar(@$resample_data), '9', "9 resample data";
};

sub ticks_from_csv {
    my $filename = 't/tickdata2.csv';

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
