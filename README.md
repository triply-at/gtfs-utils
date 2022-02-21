# gtfsutils

GTFS command-line filtering tool

## Installation

```bash
git clone git@github.com:triply-at/gtfsfilter.git
cd gtfsfilter
pip install .
```

## Usage

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
