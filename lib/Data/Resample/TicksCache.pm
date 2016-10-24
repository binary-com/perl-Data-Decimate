package Data::Resample::TicksCache;

use strict;
use warnings;

use 5.010;
use Moose;

extends 'Data::Resample';

=head1 SUBROUTINES/METHODS

=head2 tick_cache_insert

=cut

sub tick_cache_insert {
    my ($self, $tick, $fast_insert) = @_;

    $tick = $tick->as_hash if blessed($tick);

    my %to_store = %$tick;

    $to_store{count} = 1;    # These are all single ticks;
    my $key = $self->_make_key($to_store{symbol}, 0);

    return _update($self->_redis, $key, $tick->{epoch}, $encoder->encode(\%to_store), $fast_insert);
}

=head2 tick_cache_get_num_ticks

=cut

sub tick_cache_get_num_ticks {

}

no Moose;

__PACKAGE__->meta->make_immutable;

1;
