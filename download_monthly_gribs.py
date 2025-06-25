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
scratchspace: Path = SCRATCHSPACE_BASE / "scratchspace" / "era5"
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
    "surface_solar_radiation_downwards", "total_precipitation",
]
datasets_choice = click.Choice(dataset_names)

# --- Utility Functions ---
def next_month(dt: datetime) -> datetime:
    """Return the first day of the next month."""
    y, m = (dt.year, dt.month + 1) if dt.month < 12 else (dt.year + 1, 1)
    return dt.replace(year=y, month=m, day=1)

def make_grib_filepath(dataset: str, timestamp: datetime) -> Path:
    """Creates a standardized filepath for a monthly GRIB file."""
    return scratchspace / f"{dataset}-{timestamp.strftime('%Y%m')}.grib"

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
            await s3_client.download_file(era5_env["BUCKET_NAME"], filename, str(download_filepath))
            eprint(f"✅ Downloaded from R2: {filename}")
            return download_filepath

    # --- Case 4: File not found in any cache, or force=True. Download from Copernicus. ---
    eprint(f"🌍 Requesting from Copernicus: {filename}...")
    
    month_start = timestamp.replace(day=1)
    next_month_start = next_month(month_start)
    num_days = (next_month_start - month_start).days
    day_request = [f"{i+1:02d}" for i in range(num_days)]

    request = {
        "product_type": "reanalysis", "variable": dataset, "year": timestamp.strftime("%Y"),
        "month": timestamp.strftime("%m"), "day": day_request, "time": [f"{h:02d}:00" for h in range(24)],
        "format": "grib",
    }
    
    client_args = {"quiet": True, "url": "https://cds.climate.copernicus.eu/api"}
    if api_key:
        client_args['key'] = api_key
    
    # Use standard asyncio.to_thread to run the blocking download
    def _sync_cds_download():
        client = cdsapi.Client(**client_args)
        client.retrieve("reanalysis-era5-single-levels", request, str(download_filepath))

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
@click.option("--max-concurrent-downloads", type=int, default=3, show_default=True, help="Max parallel operations.")
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