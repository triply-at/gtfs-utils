# gtfs-utils

:construction: **Work in progress** - This library is being combined with [gtfsutils](https://github.com/triply-at/gtfsutils/tree/master) :construction:

Utility library for working with gtfs files

## Installation

```bash
git clone git@github.com:triply-at/gtfsfilter.git
cd gtfsfilter
pip install .
```

## Usage

Note, this syntax is outdated! Use `gtfsfilter -h` and `gtfsfilter src [subcommand] -h ` for more info.

```bash
gtfsfilter -h
```

```
usage: gtfsfilter [-h] [--bounds BOUNDS] [--overwrite] [-v] [-s] [--complete-trips] [-t] src dst

GTFS Filter

positional arguments:
  src               Input filepath
  dst               Output filepath

optional arguments:
  -h, --help        show this help message and exit
  --bounds BOUNDS   Filter boundary
  --overwrite       Overwrite if exists
  -v, --verbose     Verbose output
  -s, --shapes      Include shapes.txt
  --complete-trips  If applying bounds to a gtfs file, all trips and objects
                    outside the border are excluded; even then if they belong
                    to a trip which is not entirely excluded because it has
                    some stops inside the borders. The part of the trip inside
                    the border will be available.
                    This option will ensure that every trip stays complete.
                    That means there will be stops also outside of the bounds.
                    This option results in slower processing.
  -t, --transfers   Include transfers.txt
```

### Example usage

Unzip a gtfs file, cut it to bounds and zip it again
```
unzip latest.gtfs.zip -d latest.gtfs
gtfsfilter  latest.gtfs  extract --bounds="[7.4588,46.9844,11.6545,48.932]" --complete-trips cut.gtfs
zip -j cut.gtfs.zip cut.gtfs/*.txt
```

## Tests

### Data used

For testing purposes, GTFS data from Vienna's transit agency is used. [`GTFS Transport Schedules Vienna`]([https://data.gv.at/](https://www.data.gv.at/katalog/dataset/ab4a73b6-1c2d-42e1-b4d9-049e04889cf0)) by `Wiener Linien GmbH & Co KG` are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).