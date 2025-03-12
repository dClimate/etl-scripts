import os
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path

import click
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore

from etl_scripts.grabbag import eprint

scratchspace: Path = (Path(__file__).parent / "scratchspace" / "prism").absolute()
os.makedirs(scratchspace, exist_ok=True)

dataset_names = [
    "precip-4km",
    "tmax-4km",
    "tmin-4km",
]
datasets_choice = click.Choice(dataset_names)

dataset_to_prism_datatype = {
    "precip-4km": "ppt",
    "tmax-4km": "tmax",
    "tmin-4km": "tmin",
}


@click.command
@click.argument("dataset", type=datasets_choice)
def get_available_timespan(dataset: str):
    """
    TODO documentation
    """
    prism_datatype = dataset_to_prism_datatype[dataset]
    years_list_url = f"https://ftp.prism.oregonstate.edu/daily/{prism_datatype}/"
    years_list_subprocess = subprocess.run(
        f"curl --silent {years_list_url}"
        + r" | grep --extended-regexp '[1-2][0-9]{3}\/' | sed -E 's/.*>([1-2][0-9]{3})\/<.*/\1/' | sort",
        shell=True,
        capture_output=True,
        text=True,
    )
    if years_list_subprocess.returncode != 0:
        raise Exception("Encountered an error when getting the list of years")
    years_list = years_list_subprocess.stdout.strip().split()
    earliest_year = years_list[0]
    latest_year = years_list[-1]

    filter = r" | grep -oE '>PRISM_(ppt|tmax|tmin)_.*[0-9]{8}_bil\.zip<' | sed -E 's/.*([1-2][0-9]{3}[0-1][0-9][0-3][0-9]).*/\1/' | sort | head -n 1"
    earliest_date_subprocess = subprocess.run(
        f"curl --silent 'https://ftp.prism.oregonstate.edu/daily/{prism_datatype}/{earliest_year}/'"
        + filter,
        shell=True,
        capture_output=True,
        text=True,
    )
    if earliest_date_subprocess.returncode != 0:
        raise Exception("Error while finding the earliest date")
    latest_date_subprocess = subprocess.run(
        f"curl --silent 'https://ftp.prism.oregonstate.edu/daily/{prism_datatype}/{latest_year}/'"
        + filter,
        shell=True,
        capture_output=True,
        text=True,
    )
    if latest_date_subprocess.returncode != 0:
        raise Exception("Error while finding the latest date")

    earliest_date_str = earliest_date_subprocess.stdout.strip()
    e_y = earliest_date_str[0:4]
    e_m = earliest_date_str[4:6]
    e_d = earliest_date_str[6:8]
    latest_date_str = latest_date_subprocess.stdout.strip()
    l_y = latest_date_str[0:4]
    l_m = latest_date_str[4:6]
    l_d = latest_date_str[6:8]

    print(f"{e_y}-{e_m}-{e_d} {l_y}-{l_m}-{l_d}")


def make_nc_path(dataset: str, day: datetime, grid_count: int) -> Path:
    return scratchspace / f"{dataset}_{day.strftime('%Y-%m-%d')}_{grid_count}.nc"


def download_day(dataset: str, day: datetime, force=False) -> Path:
    """
    Downloads the nc file for that day and returns a path to it.

    Checks if PRISM
    """
    if dataset not in dataset_names:
        raise ValueError("Incorrect dataset name")

    # Check to see if a file already exists, and whether it has the latest grid count
    existing_nc_path: Path | None = None
    for grid_count in range(1, 9):
        nc_path = make_nc_path(dataset, day, grid_count)
        if nc_path.exists():
            # already at maximum, just return immediately
            if grid_count == 8:
                return nc_path
            existing_nc_path = nc_path
            break

    prism_datatype = dataset_to_prism_datatype[dataset]
    yyyymmdd = day.strftime("%Y%m%d")
    source_grid_count_query = subprocess.run(
        [
            "curl",
            "--silent",
            f"https://services.nacse.org/prism/data/public/releaseDate/{prism_datatype}/{yyyymmdd}/{yyyymmdd}",
        ],
        capture_output=True,
        text=True,
    )
    if source_grid_count_query.returncode != 0:
        raise Exception("Querying for source grid count returned an error")
    response = source_grid_count_query.stdout.strip().split()
    source_grid_count = int(response[3])
    if existing_nc_path is not None and existing_nc_path.exists():
        disk_grid_count = int(existing_nc_path.name[-4])

        if disk_grid_count > source_grid_count:
            raise Exception(
                "Grid count of file on disk is greater than source grid count"
            )

        # If we are up to date, then don't redownload the file
        if disk_grid_count == source_grid_count:
            return existing_nc_path

    # Download the file and overwrite whatever's already there
    download_url = f"https://services.nacse.org/prism/data/public/4km/{prism_datatype}/{yyyymmdd}?format=nc"
    nc_path = make_nc_path(dataset, day, source_grid_count)

    # Download the zip and unzip it
    zip_path = nc_path.with_suffix(".zip")
    extract_dir_path = nc_path.with_suffix("")
    # If a previous zip exists or extraction dir exists, then there was an issue, we should delete those
    if zip_path.exists():
        os.remove(zip_path)
    if extract_dir_path.exists():
        shutil.rmtree(extract_dir_path)

    download_subprocess = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",  # if we get a 404 then show it
            "--fail",  # return with status 22 on bad HTTP response code
            "-o",  # Specify output filepath
            zip_path,
            download_url,
        ]
    )
    if download_subprocess.returncode != 0:
        raise Exception("Error while downloading with curl")

    if zip_path.stat().st_size < 1000:
        raise Exception(
            "Downloaded zip is less than 1000 bytes, there was most likely an issue with downloading more than twice in a day"
        )

    # Unzip, extract and rename nc file, and then remove our working files
    unzip_subprocess = subprocess.run(
        [
            "unzip",
            zip_path,
            "-d",
            extract_dir_path,
        ],
        capture_output=True,  # capture unzip's comments' about the files its creating to stop it from being visible
    )
    if unzip_subprocess.returncode != 0:
        raise Exception("Error while unzipping")
    rename_subprocess = subprocess.run(
        [
            "mv",
            extract_dir_path / f"PRISM_{prism_datatype}_stable_4kmD2_{yyyymmdd}_nc.nc",
            nc_path,
        ]
    )
    if rename_subprocess.returncode != 0:
        raise Exception("Error while renaming downloaded nc file")
    os.remove(zip_path)
    shutil.rmtree(extract_dir_path)

    # PRISM asks 2 seconds between downloads to avoid overwhelming their servers
    # https://prism.oregonstate.edu/documents/PRISM_downloads_web_service.pdf
    time.sleep(2)

    return nc_path


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("day", type=click.DateTime())
def download(dataset: str, day: datetime):
    """TODO documentation,
    include fact about not downloading from source the same file twice a day"""
    download_day(dataset, day)


@click.group
def cli():
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)

if __name__ == "__main__":
    cli()
