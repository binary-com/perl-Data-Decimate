package Data::Resample;

use strict;
use warnings;

use 5.010;
use Moose;

use RedisDB;
use Date::Utility;
use Sereal::Encoder;
use Sereal::Decoder;

use MooseX::Types::Moose qw(Int Num Str);
use MooseX::Types -declare => [qw(
        interval
        )];

use Moose::Util::TypeConstraints;
use Time::Duration::Concise;

subtype 'interval', as 'Time::Duration::Concise';
coerce 'interval', from 'Str', via { Time::Duration::Concise->new(interval => $_) };

=head1 NAME

Data::Resample - A module that allows to resample a data feed. 

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

  use Data::Resample::TicksCache;
  use Data::Resample::ResampleCache;

  my $ticks_cache = Data::Resample::TicksCache->new({
        redis => $redis,
        });

  my %tick = (
        symbol => 'USDJPY',
        epoch  => time,
        quote  => 103.0,
        bid    => 103.0,
        ask    => 103.0,
  );

  $ticks_cache->tick_cache_insert(\%tick);

  my $ticks = $ticks_cache->tick_cache_get_num_ticks({
        symbol => 'USDJPY',
        });

  my $resample_cache = Data::Resample::ResampleCache->new({
        redis => $redis,
        });

=head1 DESCRIPTION

A module that allows you to resample a data feed

=cut

our $VERSION = '0.01';

=head1 ATTRIBUTES
=cut

=head2 sampling_frequency

=head2 tick_cache_size

=head2 resample_cache_size

=cut

has sampling_frequency => (
    is      => 'ro',
    isa     => 'interval',
    default => '15s',
    coerce  => 1,
);

has tick_cache_size => (
    is      => 'ro',
    default => 1860,
);

has resample_cache_size => (
    is      => 'ro',
    default => 2880,
);

has agg_retention_interval => (
    is      => 'ro',
    isa     => 'interval',
    lazy    => 1,
    coerce  => 1,
    builder => '_build_agg_retention_interval',
);

sub _build_agg_retention_interval {
    my $self = shift;
    my $interval = int($self->resample_cache_size / (60 / $self->sampling_frequency->seconds));
    return $interval . 'm';
}

has unagg_retention_interval => (
    is      => 'ro',
    isa     => 'interval',
    lazy    => 1,
    coerce  => 1,
    builder => '_build_unagg_retention_interval',
);

sub _build_unagg_retention_interval {
    my $interval = int(shift->tick_cache_size / 60);
    return $interval . 'm';
}

has decoder => (
    is      => 'ro',
    lazy    => 1,
    builder => '_build_decoder',
);

sub _build_decoder {
    return Sereal::Decoder->new;
}

has encoder => (
    is      => 'ro',
    lazy    => 1,
    builder => '_build_encoder',
);

sub _build_encoder {
    return Sereal::Encoder->new({
        canonical => 1,
    });
}

has 'redis_read' => (
    is       => 'ro',
    required => 1
);

has 'redis_write' => (
    is       => 'ro',
    required => 1
);

=head1 SUBROUTINES/METHODS

=head2 _make_key

=cut

sub _make_key {
    my ($self, $symbol, $agg) = @_;

    my @bits = ("AGGTICKS", $symbol);
    if ($agg) {
        push @bits, ($self->sampling_frequency->as_concise_string, 'AGG');
    } else {
        push @bits, ($self->unagg_retention_interval->as_concise_string, 'FULL');
    }

    return join('_', @bits);
}

=head2 _update

=cut 

sub _update {
    my ($self, $redis, $key, $score, $value) = @_;

    return $redis->zadd($key, $score, $value);
}

=head2 _check_missing_ticks

=cut

sub _check_missing_ticks {
    my ($self, $args) = @_;

    my $aggregated_data = $args->{agg_data};

    my @sorted_agg = sort { $a <=> $b } keys %$aggregated_data;
    my $first_key  = $sorted_agg[0];
    my $last_key   = $sorted_agg[-1];

    for (my $i = $first_key; $i <= $last_key; $i = $i + $self->sampling_frequency->seconds) {
        my $tick = $aggregated_data->{$i};

        if (not $tick) {
            my $tick     = $aggregated_data->{$i - $self->sampling_frequency->seconds};
            my %to_store = %$tick;
            $to_store{agg_epoch}   = $i;
            $to_store{count}       = 0;
            $aggregated_data->{$i} = \%to_store;
        }
    }

    return $aggregated_data;
}

=head2 _aggregate

=cut

sub _aggregate {
    my ($self, $args) = @_;

    my $ul       = $args->{symbol};
    my $end      = $args->{end_epoch} // time;
    my $ticks    = $args->{ticks};
    my $backtest = $args->{backtest} // 0;

    my $ai = $self->sampling_frequency->seconds;    #default 15sec

    my $agg_key = $self->_make_key($ul, 1);

    my $counter        = 0;
    my $prev_agg_epoch = 0;
    my %aggregated_data;

    if ($ticks) {
        %aggregated_data = map {
            my $agg_epoch = ($_->{epoch} % $ai) == 0 ? $_->{epoch} : $_->{epoch} - ($_->{epoch} % $ai) + $ai;
            $counter = ($agg_epoch == $prev_agg_epoch) ? $counter + 1 : 1;
            $_->{count}     = $counter;
            $_->{agg_epoch} = $agg_epoch;
            $prev_agg_epoch = $agg_epoch;
            ($agg_epoch) => $_
        } @$ticks;

    }

    my $res = $self->_check_missing_ticks({
        agg_data => \%aggregated_data,
    });

    my @sorted_agg = sort { $a <=> $b } keys %$res;

    if (not $backtest) {
        foreach my $key (@sorted_agg) {
            my $tick = $res->{$key};
            $self->_update($self->redis_write, $agg_key, $key, $self->encoder->encode($tick));
        }
    }

    my @vals = map { $res->{$_} } @sorted_agg;
    return \@vals;
}

=head1 AUTHOR

Binary.com, C<< <support at binary.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-data-resample at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Resample>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Data::Resample


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Data-Resample>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Data-Resample>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Data-Resample>

=item * Search CPAN

L<http://search.cpan.org/dist/Data-Resample/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2016 Binary.com.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

# End of Data::Resample
no Moose;

__PACKAGE__->meta->make_immutable;

1;
