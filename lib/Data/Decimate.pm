package Data::Decimate;

use strict;
use warnings;

use 5.010;

use Exporter qw/import/;

our @EXPORT_OK = qw(decimate);

=head1 NAME

Data::Decimate - A module that allows to decimate a data feed. 

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

  use Data::Decimate qw(decimate);

  my @data = [
        {epoch  => 1479203101,
        ...},
        {epoch  => 1479203102,
        ...},
        {epoch  => 1479203103,
        ...},
        ...
        {epoch  => 1479203114,
        ...},
        {epoch  => 1479203117,
        ...},
        {epoch  => 1479203118,
        ...},
        ...
  ];

  my $output = Data::Decimate::decimate(15, \@data);

  #epoch=1479203114 , decimate_epoch=1479203115
  print $output->[0]->{epoch};
  print $output->[0]->{decimate_epoch};

=head1 DESCRIPTION

A module that allows you to resample a data feed

=cut

our $VERSION = '0.01';

=head1 SUBROUTINES/METHODS
=cut

sub _fill_missing_data {
    my ($interval, $decimate_data) = @_;

    my @sorted_data = sort { $a <=> $b } keys %$decimate_data;
    my $first_key   = $sorted_data[0];
    my $last_key    = $sorted_data[-1];

    if (scalar(@sorted_data)) {
        for (my $i = $first_key; $i <= $last_key; $i = $i + $interval) {
            my $data = $decimate_data->{$i};

            if (not $data) {
                my $data     = $decimate_data->{$i - $interval};
                my %to_store = %$data;
                $to_store{decimate_epoch} = $i;
                $to_store{count}          = 0;
                $decimate_data->{$i}      = \%to_store;
            }
        }
    }

    return $decimate_data;
}

=head2 decimate

Decimate a given data based on sampling frequency.

=cut

sub decimate {
    my ($interval, $data) = @_;

    if (not defined $interval or not defined $data or ref($data) ne "ARRAY") {
        die "interval and data are required parameters.";
    }

    my $counter             = 0;
    my $prev_decimate_epoch = 0;
    my %decimate_data;

    %decimate_data = map {
        my $decimate_epoch = ($_->{epoch} % $interval) == 0 ? $_->{epoch} : $_->{epoch} - ($_->{epoch} % $interval) + $interval;
        $counter = ($decimate_epoch == $prev_decimate_epoch) ? $counter + 1 : 1;
        $_->{count}          = $counter;
        $_->{decimate_epoch} = $decimate_epoch;
        $prev_decimate_epoch = $decimate_epoch;
        $decimate_epoch => $_
    } @$data;

    my $tmp = _fill_missing_data($interval, \%decimate_data);
    my @res = map { $tmp->{$_} } sort { $a <=> $b } keys %$tmp;
    return \@res;
}

=head1 AUTHOR

Binary.com, C<< <support at binary.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-data-resample at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Decimate>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Data::Decimate


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Data-Decimate>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Data-Decimate>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Data-Decimate>

=item * Search CPAN

L<http://search.cpan.org/dist/Data-Decimate/>

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

1;
