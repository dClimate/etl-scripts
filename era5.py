import json
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Literal
import time
from zarr.storage import MemoryStore
import itertools
import dask.array as da
import subprocess
import warnings

import aioboto3
import boto3
import cdsapi
import click
import numcodecs
import numpy as np
import xarray as xr
import zarr.codecs
from botocore.exceptions import ClientError as S3ClientError
from multiformats import CID
from py_hamt import ShardedZarrStore, KuboCAS
import asyncio
from etl_scripts.grabbag import eprint, npdt_to_pydt

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


async def is_file_on_r2(key: str, r2_client) -> bool:
    """Asynchronously checks if a file exists in the R2 bucket."""
    try:
        await r2_client.head_object(Bucket=era5_env["BUCKET_NAME"], Key=key)
        return True
    except S3ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # Something else has gone wrong
            raise


async def _upload_with_r2_async(filepath: Path, bucket: str, key: str):
    """
    Reads a file and uploads it using the low-level put_object to avoid upload_file issues.
    """
    s3_config = {
        "endpoint_url": era5_env["ENDPOINT_URL"],
        "aws_access_key_id": era5_env["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": era5_env["AWS_SECRET_ACCESS_KEY"],
    }
    def _sync_upload():
        eprint(f"☁️ Starting synchronous upload in thread: {key}...")
        try:
            # Create a brand new, standard boto3 client inside the thread.
            # This client is truly synchronous and blocking.
            boto3_s3_client = boto3.client("s3", **s3_config)
            
            # This call will now BLOCK until the upload is complete, fails, or times out.
            boto3_s3_client.upload_file(str(filepath), bucket, key)
            
            eprint(f"✅ Finished synchronous upload in thread: {key}")
        except Exception as e:
            # We can catch specific botocore exceptions if needed
            eprint(f"❌ Error during synchronous upload for {key}: {e}")
            # Re-raise the exception so the main async task knows about the failure
            raise

    try:
        # Run the genuinely blocking upload function in asyncio's thread pool.
        # This await will now correctly wait for the entire file transfer.
        await asyncio.to_thread(_sync_upload)
    except Exception as e:
        # The exception from the thread is caught here.
        eprint(f"❌ Upload task for {key} failed.")
        # Re-raise it to be caught by the main result processing loop
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

async def get_gribs_for_date_range_async(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    s3_client,
    api_key: str | None = None
) -> list[Path]:
    """
    Determines the months required for a date range and ensures the monthly GRIB files
    are downloaded locally, fetching from R2 or Copernicus as needed.
    """
    required_months: Set[datetime] = set()
    current_month_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Identify all unique months spanned by the date range
    while current_month_start <= end_date:
        required_months.add(current_month_start)
        current_month_start = next_month(current_month_start)

    eprint(f"Date range requires {len(required_months)} monthly GRIB files.")
    
    # Create concurrent download tasks for each required month
    download_tasks = [
        download_grib_async(dataset, month_ts, "month", s3_client, api_key=api_key)
        for month_ts in required_months
    ]
    
    # Await all downloads to complete
    monthly_grib_paths = await asyncio.gather(*download_tasks)
    
    return monthly_grib_paths

async def download_grib_async(
    dataset: str,
    timestamp: datetime,
    period: str,
    s3_client,
    force=False,
    api_key: str | None = None,
) -> Path:
    """
    Asynchronously downloads a GRIB file, coordinating between local disk, R2, and the Copernicus API.
    """
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")
    if period not in period_options:
        raise ValueError(f"Invalid period {period}")

    download_filepath = make_grib_filepath(dataset, timestamp, period)
    # Run synchronous file check in a thread to avoid blocking the event loop
    file_on_disk = await asyncio.to_thread(download_filepath.exists)

    filename = download_filepath.name
    file_on_r2 = await is_file_on_r2(filename, s3_client)
    upload_to_r2_at_end = False

    # Decide if a fresh download from Copernicus is needed
    if force or (not file_on_r2 and not file_on_disk):
        upload_to_r2_at_end = True
    else:
        if file_on_r2 and file_on_disk:
            eprint(f"File {filename} on both R2 and disk, skipping download.")
        elif file_on_r2 and not file_on_disk:
            eprint(f"Downloading {filename} from R2 to {download_filepath}")
            await s3_client.download_file(era5_env["BUCKET_NAME"], filename, str(download_filepath))
        elif not file_on_r2 and file_on_disk:
            eprint(f"Uploading local file {download_filepath} to R2")
            await _upload_with_r2_async(str(download_filepath), era5_env["BUCKET_NAME"], filename)
        return download_filepath

    # --- Prepare and execute Copernicus API request ---
    all_hours = [f"{h:02d}:00" for h in range(24)]
    hour_request: list[str]
    day_request: list[str]

    match period:
        case "hour":
            hour_request = [timestamp.strftime("%H:00")]
            day_request = [timestamp.strftime("%d")]
        case "day":
            hour_request = all_hours
            day_request = [timestamp.strftime("%d")]
        case "month":
            hour_request = all_hours
            end = next_month(timestamp)
            delta = (end - timestamp).days
            day_request = [(timestamp + timedelta(days=i)).strftime("%d") for i in range(delta)]

    request = {
        "product_type": "reanalysis",
        "variable": dataset,
        "year": timestamp.strftime("%Y"),
        "month": timestamp.strftime("%m"),
        "day": day_request,
        "time": hour_request,
        "format": "grib", # Use 'format' for CDS API, 'data_format' is for older APIs
    }
    
    # Instantiate CDS API client
    client_args = {"quiet": True}
    if api_key:
        client_args['url'] = "https://cds.climate.copernicus.eu/api"
        client_args['key'] = api_key
    client = cdsapi.Client(**client_args)

    eprint(f"Requesting download from Copernicus to {download_filepath}...")
    
    # Run the blocking 'retrieve' call in a separate thread
    await asyncio.to_thread(
        client.retrieve,
        "reanalysis-era5-single-levels",
        request,
        str(download_filepath)
    )

    if upload_to_r2_at_end:
        eprint(f"Uploading {filename} to R2...")
        await s3_client.upload_file(str(download_filepath), era5_env["BUCKET_NAME"], filename)

    return download_filepath

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


def get_latest_timestamp(dataset: str, api_key: str | None = None) -> datetime:
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
        if api_key is None:
            client = cdsapi.Client(quiet=True)
        else:
            client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=api_key)
        result = client.retrieve("reanalysis-era5-single-levels", request)
    except Exception as e:
        exc = e
        error_message = str(exc)
        latest = error_message[-16:].replace(" ", "T")
        latest += ":00"
        return datetime.fromisoformat(latest)

@click.command
@click.argument("dataset", type=datasets_choice)
def get_available_timespan(dataset: str):
    latest = get_latest_timestamp(dataset)
    earliest = "1940-01-01T00:00:00"
    return f"{earliest} {latest}"


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
    async def main():
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=era5_env["ENDPOINT_URL"],
            aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
        ) as s3_client:
            await download_grib_async(dataset, timestamp, period, s3_client, force=force, api_key=api_key)

    asyncio.run(main())


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
            ds = ds.rename({"u100": dataset})
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



    if "valid_time" in ds.coords and len(ds.valid_time.dims) == 2:
        eprint("Processing multi-dimensional valid_time...")
        ds_stack = ds.stack(throwaway=("time", "step"))
        ds_linear = ds_stack.rename({"throwaway": "valid_time"})  # Rename stacked dim to valid_time
        ds = ds_linear.drop_vars(["throwaway"], errors="ignore")
        eprint("After handling multi-dimensional valid_time:")
        perint("Dimensions:", ds.dims)
        eprint("Coordinates:", ds.coords)

    ds = ds.drop_vars(["number", "step", "surface", "time"])
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="rename 'valid_time' to 'time'")
        # UserWarning: rename 'valid_time' to 'time' does not create an index anymore. Try using swap_dims instead or use set_index after rename to create an indexed coordinate.
        # Surpess this warning since we are renaming valid_time to time, which is the standard name in dClimate.
        # We properply set the index after renaming.
        ds = ds.rename({"valid_time": "time"})
        try:
            ds = ds.set_xindex("time")
        except ValueError:
            eprint("Time Index already exists, not setting it again.")

    # ERA5 GRIB files have longitude from 0 to 360, but dClimate standardizes from -180 to 180
    new_longitude = np.where(ds.longitude > 180, ds.longitude - 360, ds.longitude)
    ds = ds.assign_coords(longitude=new_longitude)

    ds = ds.sortby("latitude", ascending=True)
    del ds.latitude.attrs["stored_direction"]  # normally says descending
    ds = ds.sortby("longitude", ascending=True)

    # Results in about 1 MB sized chunks
    # We chunk small in spatial, wide in time
    ds = ds.chunk(chunking_settings)

    for param in list(ds.attrs.keys()):
        del ds.attrs[param]

    for var in ds.data_vars:
        da = ds[var]

        # Apply compression
        da.encoding["compressors"] = zarr.codecs.BloscCodec()

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

    """
    Creates a new, chunk-aligned Zarr store on IPFS, initialized with
    the first 1200 hours (50 days) of data.
    """
    async def main():
        # We want the initial store to contain 1200 hours of data, which is
        # exactly 3 Zarr time chunks (3 * 400h) and 50 days (50 * 24h).
        INITIAL_HOURS_TO_DOWNLOAD = 1200

        NUM_DAYS_NEEDED = INITIAL_HOURS_TO_DOWNLOAD // 24

        start_date = datetime(1940, 1, 1)
        end_date = start_date + timedelta(days=NUM_DAYS_NEEDED - 1)

        eprint(f"Downloading initial {NUM_DAYS_NEEDED} days of data to create aligned store...")

        session = aioboto3.Session()
        grib_paths: list[Path] = []
        async with session.client(
            "s3",
            endpoint_url=era5_env["ENDPOINT_URL"],
            aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
        ) as s3_client:
            try:
                # This single call replaces the old loop of 50 daily downloads.
                grib_paths = await get_gribs_for_date_range_async(
                    dataset, start_date, end_date, s3_client, api_key=api_key
                )
            except Exception as e:
                eprint(f"ERROR: A download failed while fetching monthly data for instantiation: {e}")
                sys.exit(1)

        eprint("====== Writing this dataset to a new Zarr on IPFS ======")
        ds = xr.open_mfdataset(grib_paths, decode_timedelta=False)
        slice_end_date = end_date.replace(hour=23, minute=0, second=0)
        time_slice = slice(np.datetime64(start_date), np.datetime64(slice_end_date))
        ds = ds.sel(time=time_slice)
        ds = standardize(dataset, ds)
        eprint(ds)

        eprint(f"Forcing encoding with chunk settings: {chunking_settings}")
        
        # Set encoding for the data variable
        encoding_chunks = tuple(chunking_settings.get(dim) for dim in ds[dataset].dims)
        ds[dataset].encoding['chunks'] = encoding_chunks

        for coord_name, coord_array in ds.coords.items():
            if coord_name in chunking_settings:
                ds[coord_name].encoding['chunks'] = (chunking_settings[coord_name],)

        ordered_dims = list(ds.dims)
        array_shape = tuple(ds.sizes[dim] for dim in ordered_dims)
        chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)

        # Reorder to be time, latitude, longitude
        if ordered_dims != ["time", "latitude", "longitude"]:
            ordered_dims = ["time", "latitude", "longitude"]
            ds = ds.transpose(*ordered_dims)
            array_shape = tuple(ds.sizes[dim] for dim in ordered_dims)
            chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)
        eprint("Ordered dimensions, array shape, chunk shape:")

        async def _upload_to_ipfs():
            async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
                store_write = await ShardedZarrStore.open(
                    cas=kubo_cas,
                    array_shape=array_shape,
                    chunk_shape=chunk_shape,
                    chunks_per_shard=26000,
                    read_only=False,
                )
                start_time = time.perf_counter()
                ds.to_zarr(store=store_write, mode="w")
                end_time = time.perf_counter()
                eprint(f"Time taken to write dataset: {end_time - start_time:.2f} seconds")
                root_cid = await store_write.flush()
                eprint("New ShardedZarrStore CID:")
                print(root_cid)

        await _upload_to_ipfs()

    asyncio.run(main())
    eprint("Cleaning up GRIB files...")
    # for path in grib_paths:
    #     os.remove(path)

def save_cid_to_file(
    cid: str,
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    label: str,
):
    """Saves a given CID to a text file in the scratchspace/era5/cids directory."""
    try:
        cid_dir = scratchspace / "cids"
        os.makedirs(cid_dir, exist_ok=True)

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        # Filename format: dataset-label-start_date-end_date.cid
        filename = f"{dataset}-{label}-{start_str}-to-{end_str}.cid"
        filepath = cid_dir / filename

        with open(filepath, "w") as f:
            f.write(str(cid))
        
        eprint(f"✅ Saved CID for '{label}' to: {filepath}")

    except Exception as e:
        eprint(f"⚠️ Warning: Could not save CID to file. Error: {e}")

async def chunked_write(ds: xr.Dataset, variable_name: str, kubo_cas: KuboCAS) -> str:
    # Note: I've modified it slightly to accept an existing KuboCAS instance
    #       and return the CID as a string for easier use.
    ordered_dims = list(ds.dims)
    array_shape = tuple(ds.sizes[dim] for dim in ordered_dims)
    chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)
    if ordered_dims[0] != 'time':
        ds = ds.transpose('time', 'latitude', 'longitude', ...)
        ordered_dims = list(ds.dims)
        array_shape = tuple(ds.sizes[dim] for dim in ordered_dims)
        chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)

    store_write = await ShardedZarrStore.open(
        cas=kubo_cas,
        array_shape=array_shape,
        chunk_shape=chunk_shape,
        chunks_per_shard=26000,
        read_only=False,
    )
    ds.to_zarr(store=store_write, mode="w")
    root_cid = await store_write.flush()
    return str(root_cid)

async def extend(
    dataset: str,
    cid: str,
    end_date: datetime,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
):
    """
    Extends an existing dataset to a new end date in a metadata-only operation.
    Outputs a new CID for the larger, extended (but mostly empty) dataset.
    """
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
        # --- 1. Open the initial store and read its state ---
        initial_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=cid)
        initial_ds = xr.open_zarr(initial_store)
        start_timestamp = npdt_to_pydt(initial_ds.time[-1].values)
        if end_date <= start_timestamp:
            eprint("Error: End date must be after the dataset's current last timestamp.")
            return

        initial_data_shape = initial_store._array_shape
        initial_time_coords = initial_ds.time.values
        initial_start_date = initial_ds.time.values[0] 
        initial_ds.close()

        # --- 2. Calculate the final shape and new time coordinates ---
        eprint("--- Calculating final dimensions ---")
        time_dim_index = 0 # Assuming time is the first dimension
        
        final_time_coords = np.arange(
            initial_start_date, # Use the actual start date from the original file
            np.datetime64(end_date) + np.timedelta64(1, 'h'),
            np.timedelta64(1, 'h'),
            dtype="datetime64[ns]",
        )
        
        # Calculate final shapes
        final_time_len = len(final_time_coords)
        final_data_shape_list = list(initial_data_shape)
        final_data_shape_list[time_dim_index] = final_time_len
        final_data_shape = tuple(final_data_shape_list)

        eprint(f"Will extend dataset from {initial_data_shape} to {final_data_shape}")

        eprint("--- Extending store metadata ---")
        main_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=False, root_cid=cid)

        # Step 3a: Resize the store's main shard index to the final data shape
        await main_store.resize_store(final_data_shape)

        # Step 3b: Resize the metadata of the main data variable
        await main_store.resize_variable(dataset, final_data_shape)


        # Step 3c: Write the NEW time coordinate data into the extended region
        # We must write the new time values for the extended space to be valid.
        eprint(f"Writing {len(final_time_coords)} new time values...")

        time_chunk_size = chunking_settings['time']
        time_group = zarr.open_group(main_store, mode='a')

        epoch = np.datetime64('1970-01-01T00:00:00')
        time_as_float_seconds = (final_time_coords - epoch) / np.timedelta64(1, 's')
        
        # create_dataset with overwrite=True will replace the old 'time' array entirely.
        time_array = time_group.create_dataset(
            'time',
            data=time_as_float_seconds,
            shape=time_as_float_seconds.shape,
            chunks=(time_chunk_size,),
            dtype='float64',
            dimension_names=['time'],
            overwrite=True,
            fill_value=0.0  
        )
        # Re-apply the attributes for correct decoding
        time_array.attrs['standard_name'] = 'time'
        time_array.attrs['long_name'] = 'time'
        time_array.attrs['units'] = 'seconds since 1970-01-01' # Match original units
        time_array.attrs['calendar'] = 'proleptic_gregorian'

        # Recreate the zarr.json to align with the new coordinates
        # But we don't really rely on consolidated metadata. this just keeps it tidy
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, message="Consolidated metadata is currently not part")
            zarr.consolidate_metadata(main_store)

        # --- 4. Flush and output the new CID ---
        extended_cid = await main_store.flush()
        eprint("\n--- Extend Operation Complete! ---")
        eprint("You can now use this new CID to graft data into the empty space.")
        eprint(f"Extended Dataset CID: {extended_cid}")
        return extended_cid

@click.command()
@click.argument("dataset", type=datasets_choice)
@click.option("--start-date",type=click.DateTime(), required=True, help="The first day of the batch to process.")
@click.option("--end-date", type=click.DateTime(), required=True, help="The last day of the batch to process.")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option("--api-key", help="The CDS API key to use.")
def process_batch(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
):
    """
    Downloads, processes, and creates an IPFS Zarr store for a single batch of data.
    On success, prints the final CID to standard output. All logging goes to stderr.
    This command is intended to be called as a subprocess by the 'append' command.
    """
    # Check if a CID for this exact batch has already been computed and saved.
    cid_dir = scratchspace / "cids"
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    cid_filename = f"{dataset}-batch-{start_str}-to-{end_str}.cid"
    cid_filepath = cid_dir / cid_filename

    if cid_filepath.exists() and cid_filepath.stat().st_size > 0:
        with open(cid_filepath, "r") as f:
            existing_cid = f.read().strip()
        # Ensure the file wasn't empty
        if existing_cid:
            eprint(f"✅ Found existing CID for this batch in {cid_filepath}.")
            eprint(f"Skipping processing and returning existing CID: {existing_cid}")
            print(existing_cid)  # Print to stdout for the calling process
            return
    eprint(f"--- Starting batch process for {dataset} from {start_date.date()} to {end_date.date()} ---")
    async def main():
        grib_paths: list[Path] = []
        session = aioboto3.Session()

        async with session.client(
            "s3",
            endpoint_url=era5_env["ENDPOINT_URL"],
            aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
        ) as s3_client:
            try:
                grib_paths = await get_gribs_for_date_range_async(
                    dataset, start_date, end_date, s3_client, api_key=api_key
                )
            except Exception as e:
                eprint(f"ERROR: A download failed in the batch: {e}")
                sys.exit(1)

        if not grib_paths:
            eprint("No GRIB files were downloaded, exiting.")
            sys.exit(1)

        # 3. Process and write to a new Zarr store on IPFS
        async def _process_and_upload():
            # Sort the paths by date to ensure correct time order
            grib_paths.sort(key=lambda p: p.stem)
            ds = xr.open_mfdataset(grib_paths, engine='cfgrib', decode_timedelta=False)
            slice_end_date = end_date.replace(hour=23, minute=0, second=0)
            time_slice = slice(np.datetime64(start_date), np.datetime64(slice_end_date))
            ds = ds.sel(time=time_slice)
            ds = standardize(dataset, ds)
            
            async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
                batch_cid = await chunked_write(ds, dataset, kubo_cas)
                save_cid_to_file(batch_cid, dataset, start_date, end_date, "batch")
                # CRITICAL: Print the resulting CID to standard output.
                # This is how the orchestrator will receive the result.
                print(batch_cid)
        
        try:
            await _process_and_upload()
            eprint(f"--- Batch process finished successfully for {start_date.date()} to {end_date.date()} ---")
        except Exception as e:
            eprint(f"ERROR: An error occurred during Zarr creation/upload: {e}")
            sys.exit(1)
        finally:
            # 4. Clean up downloaded GRIB files
            eprint("Cleaning up GRIB files...")
            # for path in grib_paths:
            #     try:
            #         os.remove(path)
            #     except OSError as e:
            #         eprint(f"Warning: Could not remove GRIB file {path}: {e}")
    asyncio.run(main())
    

@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.option(
    "--end-date",
    type=click.DateTime(),
    required=False,
    help="The target date to append data up to (inclusive). Format: YYYY-MM-DD",
)
@click.option("--max-parallel-procs", type=int, default=1, help="Maximum number of batch processes to run in parallel.")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
def append(
    dataset,
    cid: str,
    end_date: datetime | None,
    max_parallel_procs: int,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    api_key: str | None,
):
    """
    Append the data at timestamp onto the Dataset that cid points to, print out the CID of the new HAMT root.

    This command requires the kubo daemon to be running.

    Appends in steps of months is recommended due to Copernicus recommending month-by-month downloading from their API.
    https://confluence.ecmwf.int/display/CKB/Climate+Data+Store+%28CDS%29+documentation#ClimateDataStore(CDS)documentation-Efficiencytips
    https://forum.ecmwf.int/t/ecmwf-apis-faq-api-data-documentation/6880
    """
    if end_date is None:
        end_date = get_latest_timestamp(dataset, api_key=api_key)
    async def _do_append():
        # The batch size must be the Least Common Multiple of the data unit (24h)
        # and the Zarr chunk size (400h). LCM(24, 400) = 1200.
        HOURS_PER_BATCH = 1200
        DAYS_PER_BATCH = HOURS_PER_BATCH // 24
        main_cid = cid

        async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
            # 1. Open the initial store
            initial_store_ro = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=cid)
            if initial_store_ro._root_obj is None:
                raise ValueError("Could not load the initial store's root object. Is the CID valid?")
            initial_ds = xr.open_zarr(initial_store_ro)
            # 2. Find the end timestamp in the existing data for the starting point of the append
            start_timestamp = npdt_to_pydt(initial_ds.time[-1].values)
            eprint(f"Last timestamp in existing data: {start_timestamp.date()}")

            initial_shape = initial_store_ro._array_shape
            initial_chunks_per_dim = initial_store_ro._chunks_per_dim

            # Crucially, get the original time values as a numpy array
            initial_time_values = initial_ds.time.values
            time_dim_index = initial_ds.sizes['time']
            time_chunk_size = initial_ds.chunks['time'][0]

            # THIS OFFSET IS CRUCIAL AS IT DETERMINES WHERE THE NEW DATA WILL BE GRAFTED BEFORE THE SKELETON IS CREATED
            running_chunk_offset = initial_chunks_per_dim[0]
            print(f"Running chunk offset: {running_chunk_offset}")

            initial_ds.close()

            eprint(f"Starting append from {start_timestamp.date()}. Target end date: {end_date.date()}")

            all_days_to_download = []
            current_ts = start_timestamp
            while current_ts < end_date:
                current_ts += timedelta(days=1)
                all_days_to_download.append(current_ts)

            if not all_days_to_download:
                eprint("No new days to download.")
                return

            # We can only download in batches of 50 days, so we will split the days into batches
            batches_of_days = [
                all_days_to_download[i:i + DAYS_PER_BATCH]
                for i in range(0, len(all_days_to_download), DAYS_PER_BATCH)
            ]
            eprint(f"Planned {len(batches_of_days)} batches of up to {DAYS_PER_BATCH} days each.")

            # Create the skeleton for the new time dimension
            extended_cid = await extend(dataset, cid, end_date, gateway_uri_stem, rpc_uri_stem)
            if not extended_cid:
                eprint("Failed to extend dataset.")
                return

            async def test_append(days: list[datetime]) -> str:
                """Helper that creates one 1200-hour store and returns its CID."""
                if not days: return None

                
            # For testing, do only the first 3 batches
            start_time = time.perf_counter()
            batches_of_days = batches_of_days[:1]
            batch_cids = []

            processes = []
            batch_info = [] # To store info for each process
    
            for i, batch in enumerate(batches_of_days):
                batch_start_date = batch[0]
                batch_end_date = batch[-1]

                command = [
                    sys.executable,  # Use the same python interpreter
                    __file__,        # The current script file
                    "process-batch",
                    dataset,
                    "--start-date", batch_start_date.strftime('%Y-%m-%d'),
                    "--end-date", batch_end_date.strftime('%Y-%m-%d'),
                ]
                if gateway_uri_stem: command.extend(["--gateway-uri-stem", gateway_uri_stem])
                if rpc_uri_stem: command.extend(["--rpc-uri-stem", rpc_uri_stem])
                if api_key: command.extend(["--api-key", api_key])

                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=None, text=True)
                processes.append(proc)
                batch_info.append({'start': batch_start_date.date(), 'end': batch_end_date.date(), 'cid': None})

                if len(processes) >= max_parallel_procs:
                    # This simple implementation waits for the oldest process started
                    p_to_wait_on = processes.pop(0)
                    stdout, _ = p_to_wait_on.communicate()
                    if p_to_wait_on.returncode != 0:
                        eprint(f"ERROR: A subprocess failed with return code {p_to_wait_on.returncode}. Aborting append.")
                        # Decide on error handling: abort all or just skip this batch?
                        # For now, we'll abort.
                        raise RuntimeError("A subprocess failed. Aborting append.")
                    
                    # The CID is the stripped stdout
                    batch_cid = stdout.strip()
                    batch_info[i - len(processes)]['cid'] = batch_cid
                    eprint(f"Collected CID: {batch_cid}")

            for i, proc in enumerate(processes):
                stdout, _ = proc.communicate()
                if proc.returncode != 0:
                    eprint(f"ERROR: A subprocess failed with return code {proc.returncode}. Aborting append.")
                    raise RuntimeError("A subprocess failed. Aborting append.")
                batch_cid = stdout.strip()
                # Find the correct batch_info entry to update
                info_index = len(batch_info) - len(processes) + i
                batch_info[info_index]['cid'] = batch_cid
                eprint(f"Collected CID: {batch_cid}")

            batch_cids = [info['cid'] for info in batch_info if info['cid']]

            end_time = time.perf_counter()
            eprint(f"Time taken to create all batches: {end_time - start_time:.2f} seconds")
            eprint(f"Batch CIDs: {batch_cids}")

            # Skeleton Store to graft into
            skeleton_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=False, root_cid=CID.decode(extended_cid))
            start_time = time.perf_counter()
            for i, batch_cid in enumerate(batch_cids):
                # Calculate the offset for this graft
                running_chunk_offset += i * HOURS_PER_BATCH // 400
                current_graft_location = (running_chunk_offset, 0, 0)
                
                # Perform the graft
                await skeleton_store.graft_store(batch_cid, current_graft_location)
            end_time = time.perf_counter()
            eprint(f"Time taken to graft all batches: {end_time - start_time:.2f} seconds")
            
            # 4. Flush the main store ONCE at the end
            final_cid = await skeleton_store.flush()
            eprint(f"\nAll batches grafted successfully! Final Root CID: {final_cid}")

            eprint("Removing GRIBs on disk")
            # for path in grib_paths:
            #     os.remove(path)
    asyncio.run(_do_append())


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
cli.add_command(process_batch)

if __name__ == "__main__":
    with warnings.catch_warnings():
        # Suppress warnings about consolidated metadata not being part of the store
        # Sharding store doesnt use it
        warnings.filterwarnings("ignore", category=UserWarning, message="Consolidated metadata is currently not part")
        cli()
