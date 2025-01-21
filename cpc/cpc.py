from datetime import datetime, timezone
import os
import sys
from pathlib import Path
import subprocess

import click
import numpy as np
import xarray as xr


scratchspace: Path = (Path(__file__).parent / "scratchspace").absolute()
os.makedirs(scratchspace, exist_ok=True)

datasets_choice = click.Choice(["precip-conus", "precip-global", "tmin", "tmax"])


def download_year(dataset: str, year: int) -> Path:
    """
    Downloads the nc file for that year, and returns a path to it.

    Raises ValueError on invalid dataset. Raises Exception if download fails.
    """
    match dataset:
        case "precip-conus":
            base_url = "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_us_precip/RT/precip.V1.0."
        case "precip-global":
            base_url = "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_precip/precip."
        case "tmax":
            base_url = (
                "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmax."
            )
        case "tmin":
            base_url = (
                "https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmin."
            )
        case _:
            raise ValueError(f"Invalid dataset {dataset}")

    nc_path = scratchspace / f"{dataset}-{year}.nc"
    year_url = f"{base_url}{year}.nc"

    curl_result = subprocess.run(
        [
            "curl",
            "--silent",
            "-o",  # Specify output filepath
            nc_path,
            "-z",  # Only download if either the file does not exist or the remote file is newer
            nc_path,
            year_url,
        ]
    )
    if curl_result.returncode != 0:
        raise Exception("curl returned nonzero exit code")
    return nc_path


@click.command()
@click.argument("dataset", type=datasets_choice)
@click.pass_context
def get_available_timespan(ctx, dataset):
    """
    Gets the earliest and latest timestamps for this dataset and prints to stdout. Output looks like "earliest latest".
    """
    start_year: int
    match dataset:
        case "precip-conus":
            start_year = 2007
        case "precip-global" | "tmax" | "tmin":
            start_year = 1979
        case _:
            raise ValueError(f"Invalid dataset {dataset}")

    current_year = datetime.now(timezone.utc).year
    print(
        f"Downloading netCDF of year {current_year} to see latest data coverage",
        file=sys.stderr,
    )
    latest_nc = download_year(dataset, current_year)
    print(
        f"Downloading netCDF of start year {start_year} to see earliest data coverage",
        file=sys.stderr,
    )
    earliest_nc = download_year(dataset, start_year)
    ds_latest = xr.open_dataset(latest_nc)
    ds_earliest = xr.open_dataset(earliest_nc)

    # Sometimes, right after the start of a new year, you'll get netCDf files just with no data in them
    if len(ds_latest.time) == 0:
        print(
            f"Found no data in nc file of year {current_year}, downloading prior year",
            file=sys.stderr,
        )
        latest_nc = download_year(dataset, current_year - 1)
        ds_latest = xr.open_dataset(latest_nc)

    latest: np.datetime64 = ds_latest.time[len(ds_latest.time) - 1].values
    earliest: np.datetime64 = ds_earliest.time[0].values
    print(f"{earliest} {latest}")


@click.command()
@click.argument("dataset", type=datasets_choice)
@click.argument("timestamp", type=click.DateTime)
def download(dataset, timestamp: datetime):
    """Downloads to the scratchspace the netCDF file that contains the data for the timestamp, which should be formatted in ISO8601. This means downloading

    e.g. uv run cpc.py download precip-conus 2014-01-01
    """
    year = timestamp.year
    print(f"Downloading netCDF for year {year}", file=sys.stderr)
    download_year(dataset, year)


@click.group()
def cli():
    """
    Various commands ETLing CPC datasets. All these programs will create a scratch space folder for temporary files, named "scratchspace" located in the same directory the cpc.py file is in.
    """
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)

if __name__ == "__main__":
    cli()
