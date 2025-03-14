import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

import click
import numcodecs
import numpy as np
import pandas as pd
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
dataset_to_datatype = {
    "precip-4km": "precip",
    "tmax-4km": "temp",
    "tmin-4km": "temp",
}

# See README.md for chunking decision
chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


def add_time_dimension(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    pd_date = pd.to_datetime(date)
    ds = ds.expand_dims(time=[pd_date])
    return ds


def standardize(dataset: str, ds: xr.Dataset) -> xr.Dataset:
    del ds.attrs["GDAL"]
    del ds.attrs["history"]
    del ds.Band1.attrs["long_name"]
    del ds.Band1.attrs["grid_mapping"]
    ds = ds.drop_vars("crs")

    da = ds["Band1"]
    # Apply compression
    # clevel=9 means highest compression level (0-9 scale)
    da.encoding["compressor"] = numcodecs.Blosc(clevel=9)
    # Make fill value a float NaN instead of the original -9999.0
    da.encoding["_FillValue"] = np.nan

    ds = ds.rename({"lat": "latitude", "lon": "longitude"})
    ds = ds.sortby("latitude", ascending=True)
    ds = ds.sortby("longitude", ascending=True)
    # e.g. rename Band1 to precip
    ds = ds.rename({"Band1": dataset_to_datatype[dataset]})

    ds = ds.chunk(chunking_settings)

    return ds


def find_timespan(dataset: str) -> tuple[datetime, datetime]:
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
    e_y = int(earliest_date_str[0:4])
    e_m = int(earliest_date_str[4:6])
    e_d = int(earliest_date_str[6:8])
    latest_date_str = latest_date_subprocess.stdout.strip()
    l_y = int(latest_date_str[0:4])
    l_m = int(latest_date_str[4:6])
    l_d = int(latest_date_str[6:8])

    earliest_date = datetime(year=e_y, month=e_m, day=e_d)
    latest_date = datetime(year=l_y, month=l_m, day=l_d)

    return (earliest_date, latest_date)


@click.command
@click.argument("dataset", type=datasets_choice)
def get_available_timespan(dataset: str):
    """
    TODO documentation
    """
    earliest_date, latest_date = find_timespan(dataset)
    print(f"{earliest_date.strftime('%Y-%m-%d')} {latest_date.strftime('%Y-%m-%d')}")


def make_nc_path(dataset: str, day: datetime, grid_count: int) -> Path:
    return scratchspace / f"{dataset}_{day.strftime('%Y-%m-%d')}_{grid_count}.nc"


def find_nc_path(dataset: str, day: datetime) -> Path:
    """Only call if you are sure the file exists!"""
    for grid_count in range(1, 9):
        nc_path = make_nc_path(dataset, day, grid_count)
        if nc_path.exists():
            return nc_path


def download_day(dataset: str, day: datetime, force=False) -> Path:
    """
    Downloads the nc file for that day and returns a path to it.

    PRISM's download process is more involved than usual, so a description of the important things to know is written here.

    # bil vs nc, and FTP vs HTTPS
    PRISM provides both FTP and HTTPS services for retrieving their data.

    The FTP service is unencrypted and provides data in the `bil` format.  You can browse the FTP directories using your browser here: https://ftp.prism.oregonstate.edu/

    The HTTPS service allows for downloading in both bil and netCDF. This is why we chose the HTTPS service, as it removes a layer of complexity of converting from the bil format, and allows for encrypted file transfer, as opposed to the unencrypted FTP.
    You can see more information about the HTTPS service here: https://prism.oregonstate.edu/documents/PRISM_downloads_web_service.pdf

    # PRISM Grid Count
    Files downloaded for a dataset are named `YYYY-MM-DD_N.nc`. The YYYY-MM-DD specifies the date the data corresponds to, N is PRISM's grid count. Grid count ranges from 1-8, with a 1 specifying completely new and 8 representing finalized. PRISM assigns a higher numbers when data has been more finalized.

    See more about the grid count at https://prism.oregonstate.edu/documents/PRISM_update_schedule.pdf
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


@click.command
@click.argument("dataset", type=datasets_choice)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--skip-download",
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip downloading data. Useful when remote data provider servers are inaccessible.",
)
def instantiate(
    dataset: str, gateway_uri_stem: str, rpc_uri_stem: str, skip_download: bool
):
    num_days_needed = chunking_settings["time"]
    start_date: datetime = find_timespan(dataset)[0]
    end_date: datetime = start_date + timedelta(num_days_needed)

    # instantiate since we will need to concatenate these together
    ds_list: list[xr.Dataset] = []
    for i in range(0, num_days_needed):
        date = start_date + timedelta(days=i)
        nc_path: Path
        if skip_download:
            nc_path = find_nc_path(dataset, date)
        else:
            nc_path = download(dataset, date)
        ds = xr.open_dataset(nc_path)
        ds = add_time_dimension(ds, date)
        ds_list.append(ds)

    ds = xr.concat(ds_list, dim="time")
    ds = standardize(dataset, ds)

    eprint("====== Writing this dataset to a new Zarr on IPFS ======")
    eprint(ds)

    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem
    hamt = HAMT(store=ipfs_store)
    ds.to_zarr(store=hamt)
    eprint("HAMT CID")
    print(hamt.root_node_id)


@click.group
def cli():
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)
cli.add_command(instantiate)

if __name__ == "__main__":
    cli()
