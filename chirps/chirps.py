import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import click
import numcodecs
import numpy as np
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore

from etl_scripts.grabbag import eprint

scratchspace: Path = (Path(__file__).parent / "scratchspace").absolute()
os.makedirs(scratchspace, exist_ok=True)

datasets_choice = click.Choice(["final-p05", "final-p25", "prelim-p05"])


def make_nc_path(dataset: str, year: int) -> Path:
    return scratchspace / f"{dataset}_{year}.nc"


def make_url(dataset: str, year: int) -> str:
    base_url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0"
    url: str
    # Find these urls by just hitting index.html instead of the .nc file, e.g. https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/
    match dataset:
        case "final-p05":
            url = f"{base_url}/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc"
        case "final-p25":
            url = f"{base_url}/global_daily/netcdf/p25/chirps-v2.0.{year}.days_p25.nc"
        case "prelim-p05":
            url = f"{base_url}/prelim/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc"
        case _:
            raise ValueError(f"Invalid dataset {dataset}")

    return url


def download_year(dataset: str, year: int) -> Path:
    """
    Downloads the nc file for that year, and returns a path to it. Idempotent since it uses curl's timestamping check availability.

    Raises ValueError on invalid dataset. Raises Exception if download fails.
    """
    url = make_url(dataset, year)
    nc_path = make_nc_path(dataset, year)
    curl_result = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",  # if we get a 404 then show it
            "--fail",  # return with status 22 on bad HTTP response code
            "-o",  # Specify output filepath
            nc_path,
            "-z",  # Only download if either the file does not exist or the remote file is newer
            nc_path,
            url,
        ],
        capture_output=True,
        text=True,
    )
    if curl_result.returncode != 0:
        raise Exception("curl returned nonzero exit code")
    return nc_path


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("timestamp", type=click.DateTime())
def download(dataset: str, timestamp: datetime):
    """Download to the scratchspace the netCDF file that contains the data for the timestamp, which should be formatted in ISO8601.

    e.g. uv run cpc.py download final-p25 2014-01-01
    """
    year = timestamp.year
    eprint(f"Downloading netCDF for year {year}")
    download_year(dataset, year)


@click.command
@click.argument("dataset", type=datasets_choice)
@click.pass_context
def get_available_timespan(ctx, dataset):
    """
    Gets the earliest and latest timestamps for this dataset and prints to stdout. Output looks like "earliest latest".
    """
    start_year: int
    match dataset:
        case "prelim-p05":
            start_year = 2015
        case "final-p05" | "final-p25":
            start_year = 1981
        case _:
            raise ValueError(f"Invalid dataset {dataset}")

    current_year = datetime.now(UTC).year
    eprint(f"Checking if netCDF of current year {current_year} exists")
    current_year_nc_file_check = subprocess.run(
        ["curl", "--head", make_url(dataset, current_year)],
        capture_output=True,
        text=True,
    )
    if current_year_nc_file_check.stdout.splitlines()[0] == "HTTP/1.1 404 Not Found":
        eprint(
            "Current year nc file does not exist, going to download last year's file instead"
        )
        current_year -= 1
    eprint(f"Downloading netCDF of year {current_year} to see latest data coverage")
    latest_nc = download_year(dataset, current_year)
    eprint(
        f"Downloading netCDF of start year {start_year} to see earliest data coverage"
    )
    earliest_nc = download_year(dataset, start_year)
    ds_latest = xr.open_dataset(latest_nc)
    ds_earliest = xr.open_dataset(earliest_nc)

    if len(ds_latest.time) == 0:
        latest_nc = download_year(dataset, current_year - 1)
        ds_latest = xr.open_dataset(latest_nc)

    earliest_dt64: np.datetime64 = ds_earliest.time[0].values
    latest_dt64: np.datetime64 = ds_latest.time[len(ds_latest.time) - 1].values

    earliest_dt: datetime = datetime.fromisoformat(earliest_dt64.astype(str))
    latest_dt: datetime = datetime.fromisoformat(latest_dt64.astype(str))

    # only output in YYYY-MM-DD format for ISO8601 to reflect that CPC data only has precision in days
    earliest = earliest_dt.strftime("%Y-%m-%d")
    latest = latest_dt.strftime("%Y-%m-%d")
    print(f"{earliest} {latest}")


def standardize(ds: xr.Dataset) -> xr.Dataset:
    """Apply our standardizations to a CPC dataset."""
    # CHIRPS nc files already have the long version
    # ds = ds.rename({"lat": "latitude", "lon": "longitude"})

    # Results in about 1 MB sized chunks
    # We chunk small in spatial, wide in time
    ds = ds.chunk({"time": 1769, "latitude": 24, "longitude": 24})

    # Reorder these coordinates to be ascending order only
    ds = ds.sortby("latitude", ascending=True)
    ds = ds.sortby("longitude", ascending=True)

    for var in ds.data_vars:
        da = ds[var]

        # Apply compression
        # clevel=9 means highest compression level (0-9 scale), we are optimizing for read speed
        da.encoding["compressor"] = numcodecs.Blosc(clevel=9)

        # Prefer Fill Value over missing_value
        da.encoding["_FillValue"] = np.nan
        if "missing_value" in da.attrs:
            del da.attrs["missing_value"]
        if "missing_value" in da.encoding:
            del da.encoding["missing_value"]

    return ds


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.argument("timestamp", type=click.DateTime())
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--year",
    is_flag=True,
    show_default=True,
    default=False,
    help="Append/instantiate with the entire year that this timestamp corresponds to.",
)
@click.option(
    "--instantiate",
    is_flag=True,
    show_default=True,
    default=False,
    help="Write this timestamp to a new Zarr entirely instead of appending. If set, then the command will ignore the CID.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    show_default=True,
    default=False,
    help="Do a dry run, so load the datasets from disk and IFPS but don't actually append new data to ipfs. If instantiating, just print what we would have instantiated a new Zarr with.",
)
def append(
    dataset,
    cid: str,
    timestamp: datetime,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    instantiate: bool,
    year: bool,
    dry_run: bool,
):
    """
    Append the data at timestamp onto the Dataset that cid points to, print out the CID of the new HAMT root.

    This command requires the kubo daemon to be running.
    """
    nc_path = make_nc_path(dataset, timestamp.year)
    if not nc_path.exists():
        eprint(
            f"Did not find a netCDF file for year {timestamp.year} at the path {nc_path}"
        )
        sys.exit(1)

    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem

    ds = xr.open_dataset(nc_path)
    ds = standardize(ds)
    if year:
        ds = ds.sel(
            time=str(timestamp.year)
        )  # convert to string auto aggregate all timestamps within the year
    else:
        ds = ds.sel(time=timestamp)
    if instantiate:
        eprint("====== Writing this dataset to a new Zarr on IPFS ======")
        eprint(ds)
        if dry_run:
            sys.exit(0)
        hamt = HAMT(store=ipfs_store)
        ds.to_zarr(store=hamt)
        eprint("HAMT CID")
        print(hamt.root_node_id)
        sys.exit(0)

    eprint("====== Appending this dataset ======")
    eprint(ds)

    hamt = HAMT(store=ipfs_store, root_node_id=CID.decode(cid), read_only=False)
    ipfs_ds = xr.open_zarr(store=hamt)
    eprint("====== Loaded in this Dataset from IPFS ======")
    eprint(ipfs_ds)

    eprint("====== Appending to IPFS ======")
    if not dry_run:
        ds.to_zarr(store=hamt, mode="a", append_dim="time")
        eprint("New HAMT CID")
        print(hamt.root_node_id)
    else:
        eprint("In dry run mode, otherwise would have printed new CID here")


@click.group
def cli():
    pass


cli.add_command(download)
cli.add_command(get_available_timespan)
cli.add_command(append)

if __name__ == "__main__":
    cli()
