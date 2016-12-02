# perl-Data-Resample

A module that allows you to resample a data feed

#### SYNOPSIS

```
  use Data::Resample::TicksCache;
  use Data::Resample::ResampleCache;

  my $ticks_cache = Data::Resample::TicksCache->new({
        redis_read  => $redis,
        redis_write => $redis,
        });

  my @data_feed = [
        {symbol => 'Symbol',
        epoch  => time,
        ...},
        {symbol => 'Symbol',
        epoch  => time+1,
        ...},
        {symbol => 'Symbol',
        epoch  => time+2,
        ...},
        ...
  ];

  #Use tick_cache_insert to insert a single data
  foreach my $data (@data_feed) {
        $ticks_cache->tick_cache_insert($data);
  }

  #Use the get function to retrieve data
  my $ticks = $ticks_cache->tick_cache_get_num_ticks({
        symbol    => 'Symbol',
        end_epoch => time+3
        num       => 3,
        }););

  #Backfill function
  my $resample_cache = Data::Resample::ResampleCache->new({
        redis_read  => $redis,
        redis_write => $redis,
        });

  $resample_cache->resample_cache_backfill({
        symbol => 'Symbol',
        ticks  => \@data_feed,
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
