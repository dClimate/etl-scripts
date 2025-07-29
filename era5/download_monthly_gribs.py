#!/usr/bin/env python

"""
Standalone utility to download and cache monthly ERA5 GRIB files for a given date range.
This script is designed to "pre-warm" the local and R2 cache by downloading all
necessary monthly source files from the Copernicus Climate Data Store (CDS).
It checks for files locally and on R2 before downloading from Copernicus, and
uploads any new files to R2 for future use.
"""
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Set
import asyncio

import boto3
import aioboto3
import cdsapi
import click
from botocore.exceptions import ClientError as S3ClientError

# --- Basic Setup ---
SCRATCHSPACE_BASE = Path(__file__).parent.resolve()
scratchspace: Path = SCRATCHSPACE_BASE / "scratchspace"
os.makedirs(scratchspace, exist_ok=True)

# --- Environment and R2 Configuration ---
try:
    with open(SCRATCHSPACE_BASE / "era5-env.json") as f:
        era5_env = json.load(f)
except FileNotFoundError:
    print("Error: era5-env.json not found. Please ensure it exists in the same directory as the script.", file=sys.stderr)
    sys.exit(1)


def eprint(*args, **kwargs):
    """Prints to stderr."""
    print(*args, file=sys.stderr, **kwargs)

# --- Dataset Information ---
dataset_names = [
    "2m_temperature", "10m_u_component_of_wind", "10m_v_component_of_wind",
    "100m_u_component_of_wind", "100m_v_component_of_wind", "surface_pressure",
    "surface_solar_radiation_downwards", "total_precipitation", "land_total_precipitation"
]
datasets_choice = click.Choice(dataset_names)

# --- Utility Functions ---
def next_month(dt: datetime) -> datetime:
    """Return the first day of the next month."""
    y, m = (dt.year, dt.month + 1) if dt.month < 12 else (dt.year + 1, 1)
    return dt.replace(year=y, month=m, day=1)

def make_grib_filepath(dataset: str, timestamp: datetime) -> Path:
    """Creates a standardized filepath for a monthly GRIB file."""
    path: Path = scratchspace / dataset
    os.makedirs(path, exist_ok=True)
    # if dataset is land then its a zip
    file_end = "grib"
    if dataset.startswith("land_"):
        file_end = "zip"
    return path / f"{dataset}-{timestamp.strftime('%Y%m')}.{file_end}"

async def is_file_on_r2(key: str, r2_client) -> bool:
    """Asynchronously checks if a file exists in the R2 bucket."""
    try:
        await r2_client.head_object(Bucket=era5_env["BUCKET_NAME"], Key=key)
        return True
    except S3ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

# --- Core Asynchronous Logic ---

async def _upload_with_put_object(s3_client, filepath: Path, bucket: str, key: str):
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
            boto3_s3_client = boto3.client("s3", **s3_config)
            
            # This call will now BLOCK until the upload is complete, fails, or times out.
            boto3_s3_client.upload_file(str(filepath), bucket, key)
            
            eprint(f"✅ Finished synchronous upload in thread: {key}")
        except Exception as e:
            eprint(f"❌ Error during synchronous upload for {key}: {e}")
            raise

    try:
        # Run the genuinely blocking upload function in asyncio's thread pool.
        await asyncio.to_thread(_sync_upload)
    except Exception as e:
        eprint(f"❌ Upload task for {key} failed.")
        raise


async def _download_from_r2_sync(bucket: str, key: str, filepath: Path):
    """
    Downloads a file from R2 using the standard Boto3 client in a separate thread.
    """
    s3_config = {
        "endpoint_url": era5_env["ENDPOINT_URL"],
        "aws_access_key_id": era5_env["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": era5_env["AWS_SECRET_ACCESS_KEY"],
    }

    def _blocking_download():
        eprint(f"☁️  [Thread] Starting synchronous download: {key}...")
        try:
            # Create a standard, blocking boto3 client inside the thread
            boto3_s3_client = boto3.client("s3", **s3_config)
            
            boto3_s3_client.download_file(bucket, key, str(filepath))
            
            eprint(f"✅  [Thread] Finished synchronous download: {key}")
        except Exception as e:
            eprint(f"❌  [Thread] Error during synchronous download for {key}: {e}")
            raise

    try:
        # Run the blocking download function in asyncio's thread pool
        await asyncio.to_thread(_blocking_download)
    except Exception as e:
        eprint(f"❌ Download task for {key} failed in the main coroutine.")
        raise

async def download_grib_month_async(
    dataset: str,
    timestamp: datetime,
    s3_client,
    api_key: str | None,
    force: bool = False,
) -> Path:
    """
    Asynchronously downloads a single monthly GRIB file with robust, sequential cache checking.
    """
    download_filepath = make_grib_filepath(dataset, timestamp)
    filename = download_filepath.name

    # --- Case 1: Force download ---
    if not force:
        # --- Case 2: File exists on local disk ---
        file_on_disk = await asyncio.to_thread(download_filepath.exists)
        if file_on_disk:
            file_on_r2 = await is_file_on_r2(filename, s3_client)
            if not file_on_r2:
                await _upload_with_put_object(s3_client, download_filepath, era5_env["BUCKET_NAME"], filename)
            else:
                eprint(f"✅ Already cached on disk and R2: {filename}")
            return download_filepath

        # --- Case 3: File exists on R2 but not on disk ---
        file_on_r2 = await is_file_on_r2(filename, s3_client)
        if file_on_r2:
            eprint(f"☁️ Downloading from R2: {filename}...")
            await _download_from_r2_sync(era5_env["BUCKET_NAME"], filename, str(download_filepath))
            eprint(f"✅ Downloaded from R2: {filename}")
            return download_filepath

    # --- Case 4: File not found in any cache, or force=True. Download from Copernicus. ---
    eprint(f"🌍 Requesting from Copernicus: {filename}...")
    
    month_start = timestamp.replace(day=1)
    next_month_start = next_month(month_start)
    num_days = (next_month_start - month_start).days
    day_request = [f"{i+1:02d}" for i in range(num_days)]

    # Base request parameters common to all datasets
    request = {
        "year": timestamp.strftime("%Y"),
        "month": timestamp.strftime("%m"),
        "day": day_request,
        "time": [f"{h:02d}:00" for h in range(24)],
        "format": "grib",
    }
    
    # --- Select dataset and parameters based on variable ---
    if dataset.startswith("land_"):
        cds_dataset_name = "reanalysis-era5-land"
        # For the API request, remove the "land_" prefix from the variable name
        request["variable"] = dataset.removeprefix("land_")
    else:
        cds_dataset_name = "reanalysis-era5-single-levels"
        request["variable"] = dataset
        request["product_type"] = "reanalysis"

    client_args = {"quiet": True, "url": "https://cds.climate.copernicus.eu/api"}
    if api_key:
        client_args['key'] = api_key
    
    # Use standard asyncio.to_thread to run the blocking download
    def _sync_cds_download():
        client = cdsapi.Client(**client_args)
        client.retrieve(cds_dataset_name, request, str(download_filepath))

    await asyncio.to_thread(_sync_cds_download)
    eprint(f"✅ Downloaded from Copernicus: {filename}")

    # After a fresh download, always cache it to R2 using the robust helper.
    await _upload_with_put_object(s3_client, download_filepath, era5_env["BUCKET_NAME"], filename)

    return download_filepath

# --- Click Command-Line Interface ---
@click.command()
@click.argument("dataset", type=datasets_choice)
@click.option("--start-date", type=click.DateTime(), required=True, help="Start date (e.g., '2020-01-01').")
@click.option("--end-date", type=click.DateTime(), required=True, help="End date (inclusive, e.g., '2021-12-31').")
@click.option("--max-concurrent-downloads", type=int, default=6, show_default=True, help="Max parallel operations.")
@click.option("--force", is_flag=True, default=False, help="Force redownload from Copernicus even if file exists.")
@click.option("--api-key", help="CDS API key (overrides ~/.cdsapirc).")
def main(dataset: str, start_date: datetime, end_date: datetime, max_concurrent_downloads: int, force: bool, api_key: str | None):
    """Downloads all monthly ERA5 GRIB files for a dataset and date range in parallel."""
    
    async def run_downloads():
        required_months: Set[datetime] = set()
        current_month_start = start_date.replace(day=1)
        while current_month_start <= end_date:
            required_months.add(current_month_start)
            current_month_start = next_month(current_month_start)

        if not required_months:
            eprint("No months in the specified date range. Exiting.")
            return

        eprint(f"Found {len(required_months)} months to process between {start_date.date()} and {end_date.date()}.")
        eprint(f"Max concurrent operations set to: {max_concurrent_downloads}")

        semaphore = asyncio.Semaphore(max_concurrent_downloads)
        
        session = aioboto3.Session()
        async with session.client(
            "s3", endpoint_url=era5_env["ENDPOINT_URL"],
            aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
        ) as s3_client:

            async def download_with_semaphore(month_ts):
                async with semaphore:
                    try:
                        return await download_grib_month_async(dataset, month_ts, s3_client, api_key, force)
                    except Exception as e:
                        print(e)
                        return e

            tasks = [asyncio.create_task(download_with_semaphore(month)) for month in sorted(list(required_months))]
            
            results = await asyncio.gather(*tasks)
            
            success_count = 0
            sorted_months = sorted(list(required_months))
            for i, result in enumerate(results):
                month_str = sorted_months[i].strftime('%Y-%m')
                if isinstance(result, Exception):
                    eprint(f"❌ ERROR processing month {month_str}: {result}")
                elif isinstance(result, Path) and await asyncio.to_thread(result.exists):
                    success_count += 1
                else:
                    eprint(f"❌ UNKNOWN ERROR for month {month_str}: Task returned unexpectedly. Result: {result}")

            eprint(f"\n--- Download complete ---")
            eprint(f"Successfully processed {success_count}/{len(required_months)} monthly files.")

    asyncio.run(run_downloads())

if __name__ == "__main__":
    main()