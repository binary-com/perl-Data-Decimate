package Data::Resample::TicksCache;

use strict;
use warnings;

use 5.010;
use Moose;

use Scalar::Util qw( blessed );
use Sereal::Encoder;
use Sereal::Decoder;

extends 'Data::Resample';

=head1 SUBROUTINES/METHODS

=head2 tick_cache_insert

Also insert into resample cache if tick crosses 15s boundary.

=cut

sub tick_cache_insert {
    my ($self, $tick) = @_;

    $tick = $tick->as_hash if blessed($tick);

    my %to_store = %$tick;

    $to_store{count} = 1;    # These are all single ticks;
    my $key = $self->_make_key($to_store{symbol}, 0);

    return _update($self->_redis, $key, $tick->{epoch}, $self->encoder->encode(\%to_store));
}

=head2 tick_cache_bulk_insert

=cut

sub tick_cache_bulk_insert {
    my ($self, $ticks) = @_;

    if ($ticks) {
        foreach my $tick (@$ticks) {
            $self->tick_cache_insert($tick);
        }
    }

    return;
}

=head2 tick_cache_get

Retrieve ticks from start epoch till end opech .

=cut

sub tick_cache_get {
    my ($self, $args) = @_;
    my $symbol = $args->{symbol};
    my $start  = $args->{start_epoch} || 0;
    my $end    = $args->{end_epoch} || time;

    my $num = $end - $start;

    return tick_cache_get_num_ticks({
        symbol    => $symbol,
        end_epoch => $end,
        num       => $num,
    });
}

=head2 tick_cache_get_num_ticks

Retrieve num number of ticks from TicksCache.

=cut

sub tick_cache_get_num_ticks {

    my ($self, $args) = @_;

    my $symbol = $args->{symbol};
    my $end    = $args->{end_epoch} || time;
    my $num    = $args->{num} || 1;

    my $redis = $self->_redis;
    my @res;

    @res = map { $self->decoder->decode($_) } reverse @{$redis->zrevrangebyscore($self->_make_key($symbol, 0), $end, 0, 'LIMIT', 0, $num)};

    return \@res;
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;
