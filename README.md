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
usage: gtfsfilter [-h] [--bounds BOUNDS] [--overwrite] [-v] [-s] [-t] src dst

GTFS Filter

positional arguments:
  src              Input filepath
  dst              Output filepath

optional arguments:
  -h, --help       show this help message and exit
  --bounds BOUNDS  Filter boundary
  -v, --verbose    Verbose output
  -s, --shapes     Include shapes.txt
  -t, --transfers  Include transfers.txt
```
