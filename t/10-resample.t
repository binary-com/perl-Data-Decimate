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

subtest "ticks_cache_insert_and_retrieve" => sub {
    my $ticks_cache = Data::Resample::TicksCache->new({
        redis => $redis,
    });

    ok $ticks_cache, "TicksCache instance has been created";

    my $ticks      = ticks_from_csv();
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

#kill 9, $server->pid;
#$server->stop;

done_testing;
