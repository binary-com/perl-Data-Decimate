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

use Data::Resample;

my $data = datas_from_csv('t/sampledata.csv');
my $data_missing = datas_from_csv('t/sampledata2.csv');

subtest "resample" => sub {

    my $resample = Data::Resample->new;

    ok $resample, "ResampleCache instance has been created";

    my $output = $resample->resample({data => $data, });

    is scalar(@$data), '142', "resampled 142 data";

    is $data->[0]->{epoch}, '1479203101', "epoch is correct";

};

subtest "resample_with_missing_data" => sub {
    my $resample = Data::Resample->new;

    ok $resample, "ResampleCache instance has been created";

    my $output = $resample->resample({data => $data_missing, });

    is scalar(@$data), '142', "resampled 142 data";

    is $data->[0]->{epoch}, '1479203101', "epoch is correct";

}

sub datas_from_csv {
    my $filename = shift;

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
