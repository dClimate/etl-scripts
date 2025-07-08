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

from era5.utils import CHUNKER, dataset_names, chunking_settings, time_chunk_size

scratchspace: Path = (Path(__file__).parent.parent / "scratchspace" / "era5").absolute()
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
time_chunk_size = 500000


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
    api_key: str | None = None,
    force: bool = False
) -> list[Path]:
    """
    Determines the months required for a date range and ensures the monthly GRIB files
    are downloaded locally, fetching from R2 or Copernicus as needed.
    """

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=era5_env["ENDPOINT_URL"],
        aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
    ) as s3_client:
        try:
            required_months: Set[datetime] = set()
            current_month_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            # Identify all unique months spanned by the date range
            while current_month_start <= end_date:
                required_months.add(current_month_start)
                current_month_start = next_month(current_month_start)

            eprint(f"Date range requires {len(required_months)} monthly GRIB files.")
            
            # Create concurrent download tasks for each required month
            download_tasks = [
                download_grib_async(dataset, month_ts, "month", s3_client, api_key=api_key, force=force)
                for month_ts in required_months
            ]
            
            # Await all downloads to complete
            monthly_grib_paths = await asyncio.gather(*download_tasks)
            
            return monthly_grib_paths
        except Exception as e:
            eprint(f"ERROR: A download failed while fetching initial data: {e}")
            sys.exit(1)

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


