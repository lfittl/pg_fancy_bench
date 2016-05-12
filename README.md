# pg_fancy_bench

Like `pg_bench`, but with real data.

Currently focused on PostgreSQL databases using the [citus](https://github.com/citusdata/citus) extension for sharding across multiple nodes.

Heavily work in progress, and experimental...

## Current Datasets

* [NYC Taxi Trip Data](http://chriswhong.com/open-data/foil_nyc_taxi/) (aka `nyc_taxi_trips`, ~11GB)

## Usage

```
pg_fancy_bench -s nyc_taxi_trips -d postgres://myuser@myhost/mydb
```

## Author(s)

* Lukas Fittl

## License

MIT License

Copyright (c) 2016, Lukas Fittl <lukas@fittl.com>
pg_fancy_bench is licensed under the MIT license, see LICENSE file for details.
