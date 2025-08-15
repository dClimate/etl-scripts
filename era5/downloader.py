# downloader.py
import json
import os
import sys
import eccodes
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Literal
import dateutil
import time
from zarr.storage import MemoryStore
import itertools
import dask.array as da
import subprocess
import warnings
import tempfile
import pathlib
import zipfile

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

scratchspace: Path = (Path(__file__).parent / "scratchspace").absolute()
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

def check_finalized(path: Path) -> bool:
    """
    Robustly checks a GRIB file for finalized and preliminary data by iterating
    through every message using the low-level eccodes library.

    This avoids the interpretation issues seen with xarray/cfgrib on certain files.

    Parameters
    ----------
    path
        A Path object pointing to the GRIB file.

    Returns
    -------
    A dictionary indicating the presence of finalized (ERA5) and 
    preliminary (ERA5T) data.
    """
    unique_expvers = set()

    # if suffix is not a grib continue
    if not path.suffix == ".grib":
        eprint(f"Skipping {path} as it is not a GRIB file.")
        return False

    try:
        # Open the GRIB file in binary read mode
        with open(path, 'rb') as f:
            # Loop while there are still messages in the file
            while True:
                # Get a handle to the next GRIB message
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break  # End of file

                try:
                    # Get the value of the 'expver' key from the message
                    expver = eccodes.codes_get(gid, 'expver')
                    unique_expvers.add(expver)
                finally:
                    # Always release the message handle
                    eccodes.codes_release(gid)

    except eccodes.ECCodesError as e:
        print(f"An ECCodes error occurred: {e}")
        raise
    if "0005" in unique_expvers:
        # The file contains preliminary data.
        return False
    else:
        print(path, True)
        # The file contains no preliminary data.
        return True

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

async def _upload_with_r2_async(s3_client, filepath: Path, bucket: str, key: str):
    """
    Reads a file and uploads it using the low-level put_object to avoid upload_file issues.
    """
    if not check_finalized(filepath):
        eprint(f"Not finalized. Not caching {filepath}")
        return
 
    eprint(f"Uploading local file {filepath} to R2")
    await s3_client.upload_file(str(filepath), era5_env["BUCKET_NAME"], key)

dataset_names = [
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "surface_pressure",
    "surface_solar_radiation_downwards",
    "total_precipitation",
    "land_total_precipitation",
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

    path: Path = scratchspace / dataset
    os.makedirs(path, exist_ok=True)
    
    # Determine file extension based on dataset prefix
    file_extension = ".zip" if dataset.startswith("land_") else ".grib"
    
    match period:
        case "hour":
            # ISO8601 compatible filename, don't use the variant with dashes and colons since mac filesystem turns colons into backslashes
            # dataset-YYYYMMDDTHHMMSS.grib/.zip
            path = path / f"{dataset}-{timestamp.strftime('%Y%m%dT%H0000')}{file_extension}"
        case "day":
            # dataset-YYYYMMDD.grib/.zip
            path = path / f"{dataset}-{timestamp.strftime('%Y%m%d')}{file_extension}"
        case "month":
            # dataset-YYYYMM.grib/.zip
            path = path / f"{dataset}-{timestamp.strftime('%Y%m')}{file_extension}"

    return path

def extract_zip_to_grib(zip_path: Path) -> Path:
    """
    Extracts a zip file to a .grib file in the same location.
    
    Parameters
    ----------
    zip_path
        Path to the zip file to extract
        
    Returns
    -------
    Path to the extracted .grib file
    """
    import tempfile
    import shutil
    
    eprint(f"Extracting {zip_path.name}")
    
    # Create a temporary directory to avoid conflicts with parallel extractions
    with tempfile.TemporaryDirectory(dir=zip_path.parent) as temp_dir:
        temp_path = Path(temp_dir)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract to temporary directory
            zip_ref.extractall(temp_path)
            
            # Find the extracted .grib file
            extracted_files = zip_ref.namelist()
            grib_files = [f for f in extracted_files if f.endswith('.grib')]
            
            if not grib_files:
                raise ValueError(f"No .grib file found in {zip_path}")
            
            # Move the grib file to final location with zip-based name
            temp_grib_path = temp_path / grib_files[0]
            final_grib_path = zip_path.with_suffix('.grib')
            
            shutil.move(str(temp_grib_path), str(final_grib_path))
            return final_grib_path

async def get_gribs_for_date_range_async(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    api_key: str | None = None,
    force: bool = False
) -> list[Path]:
    """
    Determines the optimal download strategy (daily or monthly) based on the date range.
    For short ranges (< 7 days), it downloads daily GRIB files. For longer ranges,
    it fetches entire months to optimize for bulk data acquisition.
    """
    # Calculate the time difference to determine the download strategy
    time_difference = end_date - start_date
    daily_download_threshold = timedelta(days=7)

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=era5_env["ENDPOINT_URL"],
        aws_access_key_id=era5_env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=era5_env["AWS_SECRET_ACCESS_KEY"],
    ) as s3_client:
        try:
            grib_paths: list[Path]

            # If the range is short, download by the day
            if time_difference < daily_download_threshold:
                eprint("💡 Date range is less than 7 days. Using daily download strategy.")
                required_days: set[datetime] = set()
                current_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

                # Identify all unique days spanned by the date range
                while current_day <= end_date:
                    required_days.add(current_day)
                    current_day += timedelta(days=1)

                eprint(f"Date range requires {len(required_days)} daily GRIB files.")
                download_tasks = [
                    download_grib_async(dataset, day_ts, "day", s3_client, api_key=api_key, force=force)
                    for day_ts in required_days
                ]
                grib_paths = await asyncio.gather(*download_tasks)

            # Otherwise, download by the month for efficiency with large date ranges
            else:
                eprint("💡 Date range is 7 days or more. Using monthly download strategy.")
                required_months: set[datetime] = set()
                current_month_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                # Identify all unique months spanned by the date range
                while current_month_start <= end_date:
                    required_months.add(current_month_start)
                    current_month_start = next_month(current_month_start)

                eprint(f"Date range requires {len(required_months)} monthly GRIB files.")
                download_tasks = [
                    download_grib_async(dataset, month_ts, "month", s3_client, api_key=api_key, force=force)
                    for month_ts in required_months
                ]
                grib_paths = await asyncio.gather(*download_tasks)

            return grib_paths

        except Exception as e:
            eprint(f"ERROR: A download failed while fetching initial data: {e}")
            sys.exit(1)

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
            
            # download_file is a high-level, managed transfer that is
            # the most reliable way to download an object to a local file.
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
    
    # For land datasets, check if either the zip file or the extracted grib file exists
    if dataset.startswith("land_"):
        grib_filepath = download_filepath.with_suffix('.grib')
        zip_exists = await asyncio.to_thread(download_filepath.exists)
        grib_exists = await asyncio.to_thread(grib_filepath.exists)
        file_on_disk = zip_exists or grib_exists
        eprint(f"Land dataset files exist: ZIP={zip_exists}, GRIB={grib_exists}")
    else:
        grib_filepath = None
        grib_exists = False
        # Run synchronous file check in a thread to avoid blocking the event loop
        file_on_disk = await asyncio.to_thread(download_filepath.exists)

    # If it exist on the disk, check for finalization 
    # If not finalized, force download to make sure we have latest info
    # The reason being if we process the first two weeks of a month, it will never be finalized
    # Then if we run the etl again, we can't rely anyways on the stored cache as it may only have 2 weeks but we need 
    # 2 weeks now. Then we run it again, and we need the next week, we repeat the process.
    # Eventually the finalized etl will catchup to to this date, and will redownload again to make sure it has the latest cache
    # Only when the full month is finalized do we upload it
    if (file_on_disk):
        # For land datasets, check finalization on the grib file
        check_path = grib_filepath if dataset.startswith("land_") and grib_exists else download_filepath
        if not check_finalized(check_path):
            eprint("FORCING FOR NOW")
            # force = True

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
            await _download_from_r2_sync(era5_env["BUCKET_NAME"], filename, download_filepath)
            # For land datasets, extract the downloaded zip file
            if dataset.startswith("land_") and download_filepath.suffix == ".zip":
                grib_filepath = await asyncio.to_thread(extract_zip_to_grib, download_filepath)
                return grib_filepath
        elif not file_on_r2 and file_on_disk:
            await _upload_with_r2_async(s3_client, str(download_filepath), era5_env["BUCKET_NAME"], filename)

        # For land datasets, if we have a zip but no grib, extract it
        if dataset.startswith("land_") and zip_exists and not grib_exists:
            grib_filepath = await asyncio.to_thread(extract_zip_to_grib, download_filepath)
            return grib_filepath
        elif dataset.startswith("land_") and grib_exists:
            return grib_filepath
        else:
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
    
    # Instantiate CDS API client
    client_args = {"quiet": True}
    if not api_key:
        api_key = era5_env["CDS_API_KEY"]
    if api_key:
        client_args['url'] = "https://cds.climate.copernicus.eu/api"
        client_args['key'] = api_key
    client = cdsapi.Client(**client_args)

    eprint(f"Requesting download from Copernicus to {download_filepath}...")
    
    # Run the blocking 'retrieve' call in a separate thread
    await asyncio.to_thread(
        client.retrieve,
        cds_dataset_name,
        request,
        str(download_filepath)
    )

    if upload_to_r2_at_end:
        await _upload_with_r2_async(s3_client, str(download_filepath), era5_env["BUCKET_NAME"], filename)

    # Handle zip extraction for land datasets
    if dataset.startswith("land_") and download_filepath.suffix == ".zip":
        # Extract the zip file and return the path to the .grib file
        grib_filepath = await asyncio.to_thread(extract_zip_to_grib, download_filepath)
        return grib_filepath
    
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
        cid_dir = scratchspace / dataset / "cids"
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

def get_list_of_times(time_range=range(0, 24)) -> str:
        return [f"{time_index:02}:00" for time_index in time_range]




async def load_finalization_date(dataset, cdsapi_key):
    """
    Download the six most recent months of data for a small region (NYC)
        in netCDF format from ECMWF and iterate through the time dimension,
    checking for the date when expver changes from 1 to 5.
    This will indicate when ERA5 (finalized) changes to ERA5T (preliminary).
    If `ERA5Family.finalization_date` is already set, this will be skipped unless `force` is set.

    Parameters
    ----------
    end
        A datetime representing the final value of the date range requested
    force
        A bool representing whether an existing `ERA5Family.finalization_date` value should be overwritten
    """
    eprint("requesting history of NYC lat/lon to check for ERA5 finalization date")

    # Get the most recent six months of data as a single netcdf
    temp_path = tempfile.TemporaryDirectory()
    path = pathlib.Path(temp_path.name) / "era5_cutoff_date_test.nc"

    now = datetime.now()
    current_date = datetime(now.year, now.month, 1) - dateutil.relativedelta.relativedelta(months=6)
    request = {
        "product_type": "reanalysis",
        "variable": dataset,
        "date": f"{current_date.date()}/{now.date()}",
        "time": get_list_of_times(),
        # Random US location
        "area": [
            40.75,
            -74.25,
            40.75 - 0.25,
            -74.25 + 0.25,
        ],
        "format": "netcdf",
    }
    if not cdsapi_key:
        cdsapi_key = era5_env["CDS_API_KEY"]
    client_args = {"quiet": True}
    client_args['url'] = "https://cds.climate.copernicus.eu/api"
    client_args['key'] = cdsapi_key
    client = cdsapi.Client(**client_args)

    # Run the blocking 'retrieve' call in a separate thread
    await asyncio.to_thread(
        client.retrieve,
        "reanalysis-era5-single-levels",
        request,
        str(path)
    )

    try:
        test_set = xr.load_dataset(path)
    except FileNotFoundError:
        raise ValueError(
            f"Unable to open a dataset at {path}. This likely indicates that ERA5's CDS API "
            "failed to deliver the requested finalization data. Please try again later when "
            "the CDS API is less stressed."
        )
    # if expver dimension is not present,something unexpected is wrong
    # since there should be a mix of finalized and unfinalized data in the set
    if "expver" not in test_set:
        raise MissingDimensionsError(f"No expver dimension found in {path}")
    
    # find the position of the first nan in the first
    # (final, expver = 1) column of the component key values.
    # Move back one for the previous time index.
    # argmax finds the max value -- the first nan
    previous_time_idx = test_set.expver.values.argmax() - 1
    previous_time = test_set.valid_time[previous_time_idx].values
    if previous_time is None:
        raise ValueError(
            f"No finalized data detected in {path}, even though *expver* dimension is present"
        )
    inclusive_date = npdt_to_pydt(previous_time)
    return inclusive_date
