# gtfs-utils

gtfs-utils is a utility library for reading and filtering gtfs files that can handle large datasets.

## Simple example

Filter a gtfs feed by bounding box and save it to a new directory

```python
import gtfs_utils
from gtfs_utils.filter import BoundsFilter

gtfs = gtfs_utils.load_gtfs('vienna.zip', lazy=False)
filtered_gtfs = gtfs_utils.filter_gtfs(gtfs, [BoundsFilter(bounds=[16.2, 47.95, 16.35, 48.1], complete_trips=True)])
filtered_gtfs.save('vienna_filtered')
```

or as a command-line tool:

```shell
gtfs-utils filter vienna.zip -b '[16.2, 47.95, 16.35, 48.1]' --complete-trips -o vienna-filtered.zip
```

or via docker

```shell
docker run -t -v "${PWD}:/data" ghcr.io/triply-at/gtfs-utils gtfs-utils filter /data/vienna.zip -b '[16.2, 47.95, 16.35, 48.1]' --complete-trips -o /data/vienna-filtered.zip
```


## Installation

```bash
pip install gtfsutils
```

With [uv](https://docs.astral.sh/uv) you can also directly run the latest version of the tool without installing it:

```bash
uvx gtfsutils filter data -b '[16.2, 47.95, 16.35, 48.1]' --complete-trips -o vienna-filtered
```

## Usage

### Python package

Check the [tests](tests) for example usages as a python package.

### Command-line tool

```
Usage: gtfs-utils [OPTIONS] COMMAND [ARGS]...

╭─ Options ────────────────────────────────────────────────────────────────────────────────────────╮
│ --version                       Print version and exit                                           │
│ --verbose             -v        Verbose output                                                   │
│ --install-completion            Install completion for the current shell.                        │
│ --show-completion               Show completion for the current shell, to copy it or customize   │
│                                 the installation.                                                │
│ --help                          Show this message and exit.                                      │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────────────────────────╮
│ bounds        Get the bounding box of a GTFS feed                                                │
│ route-types   List existing route types and number of routes in a GTFS feed                      │
│ info          Get information about a GTFS feed                                                  │
│ filter        Filter a GTFS feed                                                                 │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
```

For each command, you can get more information by running `gtfs-utils <command> --help`.

Each command provides the following base options:

- `--lazy`: Load the GTFS feed lazily (using `dask`). This can be useful for large feeds, but is significantly slower.
- `--no-lazy`: Load the GTFS feed eagerly. This is a lot faster, but requires more memory.

#### bounds

Prints the bounding box of a gtfs feed in `[minLon, minLat, maxLon, maxLat]` format.

```shell
gtfs-utils bounds vienna.zip
> Bounding Box:   [16.19777442, 47.99950209, 16.54940197, 48.30111117]
```

#### route-types

Prints the existing route types and the number of routes for each type in a gtfs feed.

```shell
gtfs-utils route-types vienna.zip
> Route Types:    {3: 365, 0: 109, 1: 23}
```

Run `gtfs-utils route-types --help` for more options.

#### info

Prints the existing route types and the number of routes for each type in a gtfs feed.

```shell
gtfs-utils info vienna.zip
> Info on GTFS file `vienna.zip`

Bounding Box:   [16.19777442, 47.99950209, 16.54940197, 48.30111117]
Calendar date range:    15.12.2024 - 13.12.2025

            File Sizes
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ File           ┃           Rows ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ agency         │         2 rows │
│ calendar_dates │    14_033 rows │
│ calendar       │       412 rows │
│ routes         │       497 rows │
│ stop_times     │ 3_991_121 rows │
│ stops          │     4_541 rows │
│ trips          │   213_691 rows │
└────────────────┴────────────────┘

                        Route Types
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃         Route Type          ┃ Route Type ID ┃   # Routes ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│             Bus             │       3       │ 365 routes │
│ Tram, Streetcar, Light rail │       0       │ 109 routes │
│        Subway, Metro        │       1       │  23 routes │
└─────────────────────────────┴───────────────┴────────────┘
```

Run `gtfs-utils info --help` for more options.

#### filter

Filters a gtfs feed by a set of filters.

```shell
gtfs-utils filter vienna.zip -b '[16.2, 47.95, 16.35, 48.1]' --complete-trips -o vienna-filtered.zip
> Wrote output to "vienna-filtered.zip"
```

Requires a `---output/-o` option to specify the output directory or file.

Currently supported filters:

- **Bounds**: Filter by bounding box. Use the `-b` or `--bounds` option to specify the bounding box in `[minLon, minLat, maxLon, maxLat]` format.
- **Route Types**: Filter by route types. Use the `--route-types` option to specify the route types to keep.

Run `gtfs-utils filter --help` for all options.

## Development Setup

In order to get started developing, please follow the steps below:

- Clone the repository `git clone https://github.com/triply-at/gtfs-utils` and cd into the directory
- [Install uv](https://docs.astral.sh/uv/getting-started/installation/) if you haven't already
- Install the pre-commit hooks by running `uv run pre-commit install`

The commit hooks should automatically fail on any lint and formatting errors (checked through `ruff`).
You can also run the checks manually by running `uv run ruff check --fix` and `uv run ruff format`.

If you want to run the tests, you can do so by running `uv run pytest`.
By default, most tests are executed with both eager and lazy loading. 
To speed up local tests, you can run only the eager tests with  `uv run pytest -m 'not slow'`.


## Bugs

Please report any bugs that you encounter on the [issue tracker](https://github.com/triply-at/gtfs-utils/issues).
If you can, feel free to create a pull request to solve the issue. We welcome any contributions.

## License 

This project is licensed under the [MIT License](LICENSE).

```
Copyright (C) 2025 triply GmbH
Chris Stelzmüller <c.stelzmueller@triply.at>
Luis Nachtigall <l.nachtigall@triply.at>
```

### Testing Data

For testing purposes, GTFS data from Vienna's transit agency is used. [`GTFS Transport Schedules Vienna`]([https://data.gv.at/](https://www.data.gv.at/katalog/dataset/ab4a73b6-1c2d-42e1-b4d9-049e04889cf0)) by `Wiener Linien GmbH & Co KG` are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).