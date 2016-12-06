# perl-Data-Decimate

A module that allows you to decimate a data feed

#### SYNOPSIS

```
  use Data::Decimate;

  my $data_decimate = Data::Decimate->new;

  my @data_feed = [
        {epoch  => time,
        ...},
        {epoch  => time+1,
        ...},
        {epoch  => time+2,
        ...},
        ...
  ];

  my $output = $data_decimate->decimate(\@data_feed);
```

#### INSTALLATION

To install this module, run the following commands:

        perl Makefile.PL
        make
        make test
        make install

#### USAGE

```
    use Data::Decimate;
```

#### SUPPORT AND DOCUMENTATION

After installing, you can find documentation for this module with the
perldoc command.

    perldoc Data::Decimate

Copyright (C) 2016 binary.com 
