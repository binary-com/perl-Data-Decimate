package Data::Resample::DataCache;

use strict;
use warnings;

use 5.010;
use Moose;

use Scalar::Util qw( blessed );
use Sereal::Encoder;
use Sereal::Decoder;

extends 'Data::Resample';

my %prev_added_epoch;

=head1 SUBROUTINES/METHODS

=head2 data_cache_insert

Also insert into resample cache if data crosses 15s boundary.

=cut

sub data_cache_insert {
    my ($self, $data) = @_;

    $data = $data->as_hash if blessed($data);

    my %to_store = %$data;

    $to_store{count} = 1;    # These are all single data;
    my $key = $self->_make_key($to_store{symbol}, 0);

    # check for resample interval boundary.
    my $current_epoch = $data->{epoch};
    my $prev_added_epoch = $prev_added_epoch{$to_store{symbol}} // $current_epoch;

    my $boundary = $current_epoch - ($current_epoch % $self->sampling_frequency->seconds);

    if ($current_epoch > $boundary and $prev_added_epoch <= $boundary) {
        if (
            my @datas =
            map { $self->decoder->decode($_) }
            @{$self->redis_read->zrangebyscore($key, $boundary - $self->sampling_frequency->seconds - 1, $boundary)})
        {
            #do resampling
            my $agg = $self->_resample({
                symbol    => $to_store{symbol},
                end_epoch => $boundary,
                data      => \@datas,
            });
        } elsif (
            my @resample_data = map {
                $self->decoder->decode($_)
            } reverse @{
                $self->redis_read->zrevrangebyscore(
                    $self->_make_key($to_store{symbol}, 1),
                    $boundary - $self->sampling_frequency->seconds,
                    0, 'LIMIT', 0, 1
                )})
        {
            my $single_data = $resample_data[0];
            $single_data->{resample_epoch} = $boundary;
            $single_data->{count}          = 0;
            $self->_update(
                $self->redis_write,
                $self->_make_key($to_store{symbol}, 1),
                $single_data->{agg_epoch},
                $self->encoder->encode($single_data));
        }
    }

    $prev_added_epoch{$to_store{symbol}} = $current_epoch;

    return $self->_update($self->redis_write, $key, $data->{epoch}, $self->encoder->encode(\%to_store));
}

=head2 data_cache_get

Retrieve datas from start epoch till end epoch .

=cut

sub data_cache_get {
    my ($self, $args) = @_;
    my $symbol = $args->{symbol};
    my $start  = $args->{start_epoch} // 0;
    my $end    = $args->{end_epoch} // time;

    my @res = map { $self->decoder->decode($_) } @{$self->redis_read->zrangebyscore($self->_make_key($symbol, 0), $start, $end)};

    return \@res;
}

=head2 data_cache_get_num_data

Retrieve num number of data from DataCache.

=cut

sub data_cache_get_num_data {

    my ($self, $args) = @_;

    my $symbol = $args->{symbol};
    my $end    = $args->{end_epoch} // time;
    my $num    = $args->{num} // 1;

    my @res;

    @res = map { $self->decoder->decode($_) } reverse @{$self->redis_read->zrevrangebyscore($self->_make_key($symbol, 0), $end, 0, 'LIMIT', 0, $num)};

    return \@res;
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;
