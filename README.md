# perl-Data-Resample


Data::Resample

A module that allows you to resample a data feed

#### SYNOPSIS

```
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

```

#### INSTALLATION

To install this module, run the following commands:

        perl Makefile.PL
        make
        make test
        make install

#### USAGE

```
    use Data::Resample;
```

#### SUPPORT AND DOCUMENTATION

After installing, you can find documentation for this module with the
perldoc command.

    perldoc Data::Resample

Copyright (C) 2016 binary.com 
