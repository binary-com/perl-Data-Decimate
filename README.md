# perl-Data-Resample


Data::Resample

A module that allows you to resample a data feed

sampling_frequency=15
tick_cache_size = 30*60
resample_cache_size = 12*60*60
redis=xxxx

1) TICKS CACHE: Cache of all ticks in last tick_cache_size seconds

  ->tick_cache_insert(symbol,epoch, value)     WILL ALSO INSERT INTO RESAMPLE CACHE IF THE TICK CROSSES THE 15s BOUNDARY
  ->tick_cache_get(symbol,startepoch,endepoch)    
  ->tick_cache_get_num_ticks(symbol,endepoch,num)  

2) RESAMPLE CACHE

  ->resample_cache_backfill(@ticks)
  ->resample_cache_get(symbol,startepoch,endepoch) 

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
