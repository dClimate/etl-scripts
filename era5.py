import json
import os
import sys
from datetime import datetime
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


@click.command
def get_available_timespan():
    """
    Gets the earliest and latest timestamps for this dataset and prints to stdout. Output looks like "earliest latest".
    """
    stac_response = requests.get(
        "https://cds.climate.copernicus.eu/api/catalogue/v1/collections/reanalysis-era5-single-levels"
    )
    stac_response.raise_for_status()
    stac = stac_response.json()
    earliest = stac["extent"]["temporal"]["interval"][0][0]
    latest = stac["extent"]["temporal"]["interval"][0][1]
    print(f"{earliest} {latest}")


def make_grib_filepath(dataset: str, timestamp: datetime) -> Path:
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")

    # dataset-YYYYMMDDTHHMMSS.grib
    return scratchspace / f"{dataset}-{timestamp.strftime('%Y%m%dT%H%M00')}.grib"


def download_grib(dataset: str, timestamp: datetime) -> Path:
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")

    request = {
        "product_type": ["reanalysis"],
        "variable": [dataset],
        "year": [timestamp.strftime("%Y")],  # YYYY
        "month": [timestamp.strftime("%m")],  # MM
        "day": [timestamp.strftime("%d")],  # DD
        "time": [timestamp.strftime("%H:%M")],  # HH:MM
        "data_format": "grib",
        "download_format": "unarchived",
    }

    # ISO8601 compatible filename, don't use the variant with dashes and colons since mac filesystem turns colons into backslashes
    download_filepath = make_grib_filepath(dataset, timestamp)
    client = cdsapi.Client()
    print(f"=== Downloading to {download_filepath}")
    client.retrieve("reanalysis-era5-single-levels", request, download_filepath)

    return download_filepath


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("timestamp", type=click.DateTime())
def download(dataset: str, timestamp: datetime):
    """
    Downloads the GRIB file for the timestamp. Not idempotent since it works with the cdsapi.
    """
    download_grib(dataset, timestamp)


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
    ds = ds.expand_dims("time")  # make time an actual coordinate, indexed as well

    ds = ds.sortby(
        "latitude", ascending=True
    )  # Before processing, latitude goes from 90 to -90
    # ds = ds.sortby("longitude", ascending=True) # usually already sorted

    # GRIB files have longitude from 0 to 360, but dClimate standardizes from -180 to 180
    ds = ds.assign_coords(longitude=(ds.longitude - 180))

    # Results in about 1 MB sized chunks
    # We chunk small in spatial, wide in time
    ds = ds.chunk({"time": 1769, "latitude": 24, "longitude": 24})

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
    grib_path = download_grib(dataset, timestamp)

    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem

    ds = xr.open_dataset(grib_path)
    ds = standardize(dataset, ds)

    if year:
        ds = ds.sel(
            time=str(timestamp.year)
        )  # convert to string auto aggregate all timestamps within the year
    else:
        ds = ds.sel(
            time=slice(timestamp, timestamp)
        )  # without slice, time becomes a scalar and an append will not succeed

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
    """
    Various commands for ETLing ERA5. All these programs will create a scratch space directory for temporary files at ./scratchspace/era5, relative to this file's location.
    """
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)
cli.add_command(append)

if __name__ == "__main__":
    cli()
