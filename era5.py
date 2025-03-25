import json
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path

import boto3
import cdsapi
import click
import numcodecs
import numpy as np
import xarray as xr
from botocore.exceptions import ClientError as S3ClientError
from multiformats import CID
from py_hamt import HAMT, IPFSStore, IPFSZarr3

from etl_scripts.grabbag import eprint

scratchspace: Path = (Path(__file__).parent / "scratchspace" / "era5").absolute()
os.makedirs(scratchspace, exist_ok=True)

era5_env: dict[str, str]
with open(Path(__file__).parent / "era5-env.json") as f:
    era5_env = json.load(f)

r2 = boto3.client(
    service_name="s3",
    endpoint_url=era5_env["ENDPOINT_URL"],
    aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
)


def is_file_on_r2(key: str) -> bool:
    try:
        r2.head_object(Bucket=era5_env["BUCKET_NAME"], Key=key)
        return True
    except S3ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # Something else has gone wrong
            raise


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
period_options = ["hour", "day", "month"]
period_choice = click.Choice(period_options)

chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


def next_month(dt: datetime) -> datetime:
    """Return the next month"""
    y = dt.year
    m = dt.month
    if m == 12:
        y += 1
        m = 1
    else:
        m += 1
    return dt.replace(year=y, month=m)


def make_grib_filepath(dataset: str, timestamp: datetime, period: str) -> Path:
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")
    if period not in period_options:
        raise ValueError(f"Invalid period {period}")

    path: Path = scratchspace
    match period:
        case "hour":
            # ISO8601 compatible filename, don't use the variant with dashes and colons since mac filesystem turns colons into backslashes
            # dataset-YYYYMMDDTHHMMSS.grib
            path = path / f"{dataset}-{timestamp.strftime('%Y%m%dT%H0000')}.grib"
        case "day":
            # dataset-YYYYMMDD.grib
            path = path / f"{dataset}-{timestamp.strftime('%Y%m%d')}.grib"
        case "month":
            # dataset-YYYYMM.grib
            path = path / f"{dataset}-{timestamp.strftime('%Y%m')}.grib"

    return path


def download_grib(
    dataset: str,
    timestamp: datetime,
    period: str,
    force=False,
    api_key: str | None = None,
) -> Path:
    """
    See the download command for more on the logic of this function. That click command just exists as a wrapper to expose this to the CLI.

    only_hour specifies if to only download an hour's worth of data, rather than just the whole day that hour belongs to.
    """
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")
    if period not in period_options:
        raise ValueError(f"Invalid period {period}")

    download_filepath = make_grib_filepath(dataset, timestamp, period)
    file_on_disk = download_filepath.exists()

    filename = download_filepath.name
    file_on_r2 = is_file_on_r2(filename)

    upload_to_r2_at_end = False

    # If we're either being forced to redownload, or the file is not on both R2 and disk, then redownload and upload to R2 when done
    if force or (not file_on_r2 and not file_on_disk):
        upload_to_r2_at_end = True
    else:
        if file_on_r2 and file_on_disk:
            eprint(f"File {filename} on both R2 and disk, skipping download")
            pass
        elif file_on_r2 and not file_on_disk:
            eprint(f"Downloading from R2 to local filepath {download_filepath}")
            r2.download_file(era5_env["BUCKET_NAME"], filename, download_filepath)
        elif not file_on_r2 and file_on_disk:
            eprint(f"Uploading from local file {download_filepath} to R2")
            r2.upload_file(download_filepath, era5_env["BUCKET_NAME"], filename)
        return download_filepath

    # List of times in the format HH:MM
    hour_request: list[str] = []
    day_request: list[str] = []
    all_hours = hour_request = [
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
    match period:
        case "hour":
            hour_request = [timestamp.strftime("%H:00")]
            day_request = [timestamp.strftime("%d")]
        case "day":
            hour_request = all_hours
            day_request = [timestamp.strftime("%d")]
        case "month":
            hour_request = all_hours
            # doing a .replace returns a new datetime entirely, so we can just assign to our end date like this
            end = next_month(timestamp)
            delta = (end - timestamp).days
            for i in range(0, delta):
                day_request.append((timestamp + timedelta(days=i)).strftime("%d"))

    request = {
        "product_type": ["reanalysis"],
        "variable": [dataset],
        "year": [timestamp.strftime("%Y")],  # YYYY
        "month": [timestamp.strftime("%m")],  # MM
        "day": day_request,  # list in DD format
        "time": hour_request,
        "data_format": "grib",
        "download_format": "unarchived",
    }

    client: cdsapi.Client
    if api_key is None:
        client = cdsapi.Client()
    else:
        client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=api_key)
    print(f"=== Downloading to {download_filepath}")
    # Note that this will overwrite an existing GRIB file, that's the CDS API default behavior when specifying the same filepath
    client.retrieve("reanalysis-era5-single-levels", request, download_filepath)

    if upload_to_r2_at_end:
        eprint(f"Uploading from local file {download_filepath} to R2")
        r2.upload_file(download_filepath, era5_env["BUCKET_NAME"], filename)

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
@click.argument("period", type=period_choice)
@click.option(
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force a redownload even if the data exists on disk and/or R2. This will also overwrite any preexisting files on both disk and R2.",
)
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
def download(
    dataset: str, timestamp: datetime, period: str, force: bool, api_key: str | None
):
    """
    Downloads the GRIB file for the timestamp. The period specifies whether to download data for the hour, day, or month that timestamp belongs in.

    If the file is on R2 and not on disk, this downloads to disk.
    If the file is on disk and not on R2, then this will upload a copy to R2.
    If the file is in neither locations, this will download to disk and then upload to R2.
    """
    download_grib(dataset, timestamp, period, force=force, api_key=api_key)


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
    ds = ds.set_xindex("time")

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
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
def instantiate(
    dataset: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
):
    time_chunk = chunking_settings["time"]
    num_days_needed = ceil(time_chunk / 24)
    # start and end date are an inclusive range
    start_date: datetime = datetime.fromisoformat(
        "1940-01-01T00:00:00"
    )  # constant for all datasets

    # end_date is exclusive
    end_date: datetime = start_date + timedelta(days=num_days_needed)

    # Download the months that contain the start to end date
    grib_paths: list[Path] = []
    current = start_date
    while current < end_date:
        eprint(f"Downloading GRIB for month of date {current}")
        path = download_grib(dataset, current, "month", api_key=api_key)
        grib_paths.append(path)
        current = next_month(current)

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
    ipfszarr3 = IPFSZarr3(hamt)
    ds.to_zarr(store=ipfszarr3)  # type: ignore
    eprint("HAMT CID")
    print(ipfszarr3.hamt.root_node_id)

    eprint("Now cleaning up GRIB files on disk by deleting them")
    for path in grib_paths:
        os.remove(path)


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.argument("timestamp", type=click.DateTime())
@click.argument("period", type=period_choice)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
@click.option(
    "--count",
    default=1,
    show_default=True,
    help="The number of months/days/hours to append. Usually 1, but if increased append will repeatedly print to stdout the CID of each successive append. This will essentially repeat the normal 1 count append command.",
)
@click.option(
    "--stride",
    default=1,
    show_default=True,
    help="The number of months/days/hours to append, all at once. This option is here since appending one by one takes longer than appending multiple days at a time. Stride must be a divisor of count to be valid.",
)
def append(
    dataset,
    cid: str,
    timestamp: datetime,
    period: str,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    api_key: str | None,
    count: int,
    stride: int,
):
    """
    Append the data at timestamp onto the Dataset that cid points to, print out the CID of the new HAMT root.

    This command requires the kubo daemon to be running.

    Appends in steps of months is recommended due to Copernicus recommending month-by-month downloading from their API.
    https://confluence.ecmwf.int/display/CKB/Climate+Data+Store+%28CDS%29+documentation#ClimateDataStore(CDS)documentation-Efficiencytips
    https://forum.ecmwf.int/t/ecmwf-apis-faq-api-data-documentation/6880
    """
    if stride < 1:
        return ValueError("Stride cannot be less than 1")
    if count % stride != 0:
        return ValueError("Count must be a multiple of stride")

    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem
    hamt = HAMT(store=ipfs_store, root_node_id=CID.decode(cid))
    ipfszarr3 = IPFSZarr3(hamt)

    for c in range(0, count, stride):
        eprint("====== Creating dataset for append ======")
        working_timestamps: list[datetime] = [timestamp]
        for s in range(
            1, stride
        ):  # start range from 1 since we already have the current timestamp in there
            latest = working_timestamps[-1]
            match period:
                case "hour":
                    working_timestamps.append(latest + timedelta(hours=1))
                case "day":
                    working_timestamps.append(latest + timedelta(days=1))
                case "month":
                    working_timestamps.append(next_month(latest))
        next_starting_timestamp = working_timestamps[-1]
        match period:
            case "hour":
                next_starting_timestamp = next_starting_timestamp + timedelta(hours=1)
            case "day":
                next_starting_timestamp = next_starting_timestamp + timedelta(days=1)
            case "month":
                next_starting_timestamp = next_month(next_starting_timestamp)

        # Keep a total list of all GRIBs downloaded for removal from disk at the end
        grib_paths: list[Path] = []
        for ts in working_timestamps:
            grib_path = download_grib(dataset, ts, period, api_key=api_key)
            grib_paths.append(grib_path)

        ds = xr.open_mfdataset(grib_paths)
        ds = standardize(dataset, ds)
        # fix issues with zarr chunks and dask chunks overlapping
        first_ds = xr.open_zarr(store=hamt)
        ds_rechunked = (
            xr.concat([first_ds, ds], dim="time")
            .chunk(chunking_settings)
            .sel(time=slice(ds.coords["time"][0], None))
        )
        ds = ds_rechunked
        eprint(ds)

        eprint("====== Appending to IPFS ======")
        ds.to_zarr(store=ipfszarr3, append_dim="time")  # type: ignore
        eprint(
            f"= New HAMT CID after appending from {working_timestamps[0]} to {working_timestamps[-1]} with a period of {period}"
        )
        print(ipfszarr3.hamt.root_node_id)

        eprint("Removing GRIBs on disk")
        for path in grib_paths:
            os.remove(path)

        # increment to to the next starting month for our next stride calculation
        timestamp = next_starting_timestamp


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
