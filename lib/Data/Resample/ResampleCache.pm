package Data::Resample::ResampleCache;

use strict;
use warnings;

use 5.010;
use Moose;

use List::Util qw( first min max );

extends 'Data::Resample';

=head1 SUBROUTINES/METHODS

=head2 resample_cache_backfill

=cut

sub resample_cache_backfill {
    my ($self, $symbol, $ticks) = @_;

    my $ticks_key = $self->_make_key($symbol, 0);
    foreach my $tick (@$ticks) {
        $self->_add($tick, $ticks_key, $fast_insert);
    }

    return $self->_aggregate({
        symbol    => $symbol,
        end_epoch => $end,
        ticks     => $ticks,
    });

    return;
}

=head2 resample_cache_get

=cut

sub resample_cache_get {
    my ($self, $args) = @_;

    my $which = $args->{symbol};
    my $start = $args->{start_epoch};
    my $end   = $args->{end_epoch} || time;

    my $ti    = $self->agg_retention_interval;
    my $redis = $self->_redis;

    my @res;

    $key = $self->_make_key($which, 1);

    my $start = $end - $ti->seconds;

    @res = map { $self->decoder->decode($_) } @{$redis->zrangebyscore($key, $start, $end)};

    return \@res;
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;
