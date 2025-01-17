import datetime
import os
from pathlib import Path
import subprocess

import click
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
    click.echo(f"Getting timespan for CPC dataset {dataset}...")
    start_year: int
    match dataset:
        case "precip-conus":
            start_year = 2007
        case "precip-global" | "tmax" | "tmin":
            start_year = 1979
        case _:
            raise ValueError(f"Invalid dataset {dataset}")

    current_year = datetime.datetime.now(datetime.UTC).year
    click.echo(f"Downloading netCDF of year {current_year} to see latest data coverage")
    latest_nc = download_year(dataset, current_year)
    click.echo(
        f"Downloading netCDF of start year {start_year} to see earliest data coverage"
    )
    earliest_nc = download_year(dataset, start_year)
    ds_latest = xr.open_dataset(latest_nc)
    ds_earliest = xr.open_dataset(earliest_nc)

    # Sometimes, right after the start of a new year, you'll get netCDf files just with no data in them
    if len(ds_latest.time) == 0:
        click.echo(
            f"Found no data in nc file of year {current_year}, downloading prior year"
        )
        latest_nc = download_year(dataset, current_year - 1)
        ds_latest = xr.open_dataset(latest_nc)

    latest = ds_latest.time[len(ds_latest.time) - 1].values
    earliest = ds_earliest.time[0].values
    click.secho("earliest latest", fg="blue")
    print(f"{earliest} {latest}")


@click.group()
def cli():
    pass


cli.add_command(get_available_timespan)

if __name__ == "__main__":
    cli()
