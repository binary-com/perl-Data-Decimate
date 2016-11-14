package Data::Resample;

use strict;
use warnings;

use 5.010;
use Moose;

use Cache::RedisDB;
use Date::Utility;
use Sereal::Encoder;
use Sereal::Decoder;

=head1 NAME

Data::Resample 

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Data::Resample;

    my $foo = Data::Resample->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=cut

=head1 ATTRIBUTES
=cut

=head2 sampling_frequency

=head2 tick_cache_size

=head2 resample_cache_size

=cut

has sampling_frequency  => (is => 'ro');
has tick_cache_size     => (is => 'ro');
has resample_cache_size => (is => 'ro');

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

=head1 SUBROUTINES/METHODS

=head2 _redis

=cut

sub _redis {
    return Cache::RedisDB->redis;
}

=head2 _make_key

=cut

sub _make_key {
    my ($self, $symbol, $agg) = @_;

    my @bits = ("AGGTICKS", $symbol);
    if ($agg) {
        push @bits, ($self->sampling_frequency, 'AGG');
    } else {
        push @bits, ('31m', 'FULL');
    }

    return join('_', @bits);
}

=head2 _update

=cut 

sub _update {
    my ($redis, $key, $score, $value) = @_;

    return $redis->zremrangebyscore($key, $score, $score);
}

=head2 _aggregate

=cut

sub _aggregate {
    my ($self, $args) = @_;

    my $ul    = $args->{symbol};
    my $end   = $args->{end_epoch} || time;
    my $ticks = $args->{ticks};

    my $ai = 15;                          #15sec
    my $last_agg = $end - ($end % $ai);

    my ($total_added, $first_added, $last_added) = (0, 0, 0);
    my $redis = $self->_redis;

    my ($unagg_key, $agg_key) = map { $self->_make_key($ul, $_) } (0 .. 1);

    my $count = 0;

    if ($ticks) {

        # While we are here, clean up any particularly old stuff
        $redis->zremrangebyscore($unagg_key, 0, $end - $self->unagg_retention_interval->seconds);
        $redis->zremrangebyscore($agg_key,   0, $end - $self->agg_retention_interval->seconds);
    }

    return ($total_added, Date::Utility->new($first_added), Date::Utility->new($last_added));

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
