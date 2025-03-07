import json
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path

import cdsapi
import click
import numcodecs
import numpy as np
import requests
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore

from etl_scripts.grabbag import eprint

scratchspace: Path = (Path(__file__).parent / "scratchspace" / "era5").absolute()
os.makedirs(scratchspace, exist_ok=True)

dataset_names = [
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "surface_pressure",
    "surface_solar_radiation_downwards",
    "total_precipitation",
]
datasets_choice = click.Choice(dataset_names)

chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


def make_grib_filepath(
    dataset: str, timestamp: datetime, only_hour: bool = False
) -> Path:
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")

    path: Path
    if only_hour:
        # ISO8601 compatible filename, don't use the variant with dashes and colons since mac filesystem turns colons into backslashes
        # dataset-YYYYMMDDTHHMMSS.grib
        path = scratchspace / f"{dataset}-{timestamp.strftime('%Y%m%dT%H0000')}.grib"
    else:
        # dataset-YYYYMMDD.grib
        path = scratchspace / f"{dataset}-{timestamp.strftime('%Y%m%d')}.grib"

    return path


def download_grib(
    dataset: str, timestamp: datetime, only_hour: bool = False, force=False
) -> Path:
    """
    only_hour specifies if to only download an hour's worth of data, rather than just the whole day that hour belongs to.
    If force is true, this always redownloads the data. Otherwise, it determines if the data is prelminiary using GRIB_expver, and if the data is finalized it will skip downloading.
    """
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")

    download_filepath = make_grib_filepath(dataset, timestamp, only_hour=only_hour)
    if not force and download_filepath.exists():
        return download_filepath

    # List of times in the format HH:MM
    request_times: list[str]
    if only_hour:
        request_times = [timestamp.strftime("%H:00")]
    else:
        # All 24 hours
        request_times = [
            "00:00",
            "01:00",
            "02:00",
            "03:00",
            "04:00",
            "05:00",
            "06:00",
            "07:00",
            "08:00",
            "09:00",
            "10:00",
            "11:00",
            "12:00",
            "13:00",
            "14:00",
            "15:00",
            "16:00",
            "17:00",
            "18:00",
            "19:00",
            "20:00",
            "21:00",
            "22:00",
            "23:00",
        ]

    request = {
        "product_type": ["reanalysis"],
        "variable": [dataset],
        "year": [timestamp.strftime("%Y")],  # YYYY
        "month": [timestamp.strftime("%m")],  # MM
        "day": [timestamp.strftime("%d")],  # DD
        "time": request_times,
        "data_format": "grib",
        "download_format": "unarchived",
    }

    client = cdsapi.Client()
    print(f"=== Downloading to {download_filepath}")
    # Note that this will overwrite an existing GRIB file, that's the CDS API default behavior when specifying the same filepath
    client.retrieve("reanalysis-era5-single-levels", request, download_filepath)

    return download_filepath


@click.command
@click.argument("dataset", type=datasets_choice)
def get_available_timespan(dataset: str):
    latest: str
    try:
        # If you send a request for a time beyond what is available, the API will tell you what is the latest available in an error message
        request = {
            "product_type": ["reanalysis"],
            "variable": [dataset],
            "year": ["3000"],  # YYYY
            "month": ["01"],  # MM
            "day": ["01"],  # DD
            "time": ["00:00"],  # HH:MM
            "data_format": "grib",
            "download_format": "unarchived",
        }
        client = cdsapi.Client(quiet=True)
        result = client.retrieve("reanalysis-era5-single-levels", request)
    except Exception as e:
        exc = e
        error_message = str(exc)
        latest = error_message[-16:].replace(" ", "T")
        latest += ":00"

        # A constant since all data starts at this time for ERA5
        earliest = "1940-01-01T00:00:00"

        print(f"{earliest} {latest}")


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("timestamp", type=click.DateTime())
@click.option(
    "--only-hour",
    is_flag=True,
    show_default=True,
    default=False,
    help="Download only an hour of the data, rather than the whole day.",
)
@click.option(
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force a redownload even if the data exists.",
)
def download(dataset: str, timestamp: datetime, only_hour: bool, force: bool):
    """
    Downloads the GRIB file for the timestamp. Only downloads if the file does not already exist. By default since this downloads a day of data, this ignores the hour/minute/second field of the timestamp argument.
    """
    download_grib(dataset, timestamp, only_hour=only_hour, force=force)


@click.command
@click.argument(
    "path",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
def check_finalized(path: Path):
    """Checks if an ERA5 GRIB data file has finalized data. Assumes there is only one data variable inside the GRIB file, which is the case usually for ERA5. Writes true if finalized, false if preliminary."""
    # assume path exist due to click argument verification

    ds = xr.open_dataset(path, backend_kwargs={"read_keys": ["expver"]})

    is_finalized: bool = False
    for v in ds.data_vars:
        is_finalized = int(ds[v].GRIB_expver) == 1

    if is_finalized:
        print("true")
    else:
        print("false")


def standardize(dataset: str, ds: xr.Dataset) -> xr.Dataset:
    # ERA5 normally spits things out with the short form name, use the long form name to make it more readable
    match dataset:
        case "2m_temperature":
            ds = ds.rename({"t2m": dataset})
        case "10m_u_component_of_wind":
            ds = ds.rename({"u10": dataset})
        case "10m_v_component_of_wind":
            ds = ds.rename({"v10": dataset})
        case "100m_u_component_of_wind":
            ds = ds.rename({"v100": dataset})
        case "100m_v_component_of_wind":
            ds = ds.rename({"v100": dataset})
        case "surface_pressure":
            ds = ds.rename({"sp": dataset})
        case "surface_solar_radiation_downwards":
            ds = ds.rename({"ssrd": dataset})
        case "total_precipitation":
            ds = ds.rename({"tp": dataset})
        case _:
            raise ValueError(f"Invalid dataset value {dataset}")

    ds = ds.drop_vars(["number", "step", "surface", "time"])
    ds = ds.rename({"valid_time": "time"})

    ds = ds.sortby("latitude", ascending=True)
    del ds.latitude.attrs["stored_direction"]  # normally says descending
    ds = ds.sortby("longitude", ascending=True)

    # ERA5 GRIB files have longitude from 0 to 360, but dClimate standardizes from -180 to 180
    ds = ds.assign_coords(longitude=(ds.longitude - 180))

    # Results in about 1 MB sized chunks
    # We chunk small in spatial, wide in time
    ds = ds.chunk(chunking_settings)

    for param in list(ds.attrs.keys()):
        del ds.attrs[param]

    for var in ds.data_vars:
        da = ds[var]

        # Apply compression
        # clevel=9 means highest compression level (0-9 scale)
        da.encoding["compressor"] = numcodecs.Blosc(clevel=9)

        for param in list(da.attrs.keys()):
            if param.startswith("GRIB"):
                del da.attrs[param]

        # fill value should be float NaN
        da.encoding["_FillValue"] = np.nan

    return ds


@click.command
@click.argument("dataset", type=datasets_choice)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
def instantiate(
    dataset: str,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
):
    time_chunk = chunking_settings["time"]
    num_days_needed = ceil(time_chunk / 24)
    # start and end date are an inclusive range
    start_date: datetime = datetime.fromisoformat(
        "1940-01-01T00:00:00"
    )  # constant for all datasets
    end_date: datetime = start_date + timedelta(days=num_days_needed)

    grib_paths: list[Path] = []
    for i in range(num_days_needed):
        date = start_date + timedelta(days=i)
        path: Path
        eprint(f"Downloading GRIB for date {date}")
        path = download_grib(dataset, date)
        grib_paths.append(path)

    eprint("====== Writing this dataset to a new Zarr on IPFS ======")
    ds = xr.open_mfdataset(grib_paths)
    ds = standardize(dataset, ds)
    eprint(ds)

    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem
    hamt = HAMT(store=ipfs_store)
    ds.to_zarr(store=hamt, write_empty_chunks=False)
    eprint("HAMT CID")
    print(hamt.root_node_id)


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.argument("timestamp", type=click.DateTime())
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--only-hour",
    is_flag=True,
    show_default=True,
    default=False,
    help="Only append an hour's worth of data.",
)
def append(
    dataset,
    cid: str,
    timestamp: datetime,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    only_hour: bool,
):
    """
    Append the data at timestamp onto the Dataset that cid points to, print out the CID of the new HAMT root.

    This command requires the kubo daemon to be running.
    """
    eprint("====== Creating dataset for append ======")
    eprint(f"Downloading GRIB for whole day at timestamp {timestamp}")
    grib_path = download_grib(dataset, timestamp, only_hour=only_hour)
    ds = xr.open_dataset(grib_path)
    ds = standardize(dataset, ds)
    eprint(ds)

    eprint("====== Appending to IPFS ======")
    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem
    hamt = HAMT(store=ipfs_store, root_node_id=CID.decode(cid), read_only=False)
    ds.to_zarr(store=hamt, append_dim="time", write_empty_chunks=False)
    eprint("New HAMT CID")
    print(hamt.root_node_id)


@click.group
def cli():
    """
    Commands for ETLing ERA5. On invocation, a scratch space directory relative to this file will be created for data files at ./scratchspace/era5.
    """
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)
cli.add_command(check_finalized)
cli.add_command(instantiate)
cli.add_command(append)

if __name__ == "__main__":
    cli()
