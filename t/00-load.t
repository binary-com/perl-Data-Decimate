#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Data::Decimate' ) || print "Bail out!\n";
}

diag( "Testing Data::Decimate $Data::Decimate::VERSION, Perl $], $^X" );
