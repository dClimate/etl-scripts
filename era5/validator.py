# validator.py
import json
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Literal, List, Optional
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
from era5.downloader import get_gribs_for_date_range_async
import asyncio
from etl_scripts.grabbag import eprint, npdt_to_pydt
from era5.standardizer import standardize
from era5.utils import chunking_settings, time_chunk_size

async def validate_data(
    grib_paths: List[Path],
    start_date: datetime,
    end_date: datetime,
    dataset: str,
    api_key: Optional[str] = None,
    force_download: bool = False,
    **kwargs
) -> xr.Dataset:
    """
    Validates and processes GRIB files for a given date range, ensuring the dataset has the expected number of hourly timestamps.

    Args:
        grib_paths (List[Path]): List of paths to GRIB files to process.
        start_date (datetime): Start date of the data range (inclusive, expected at 00:00:00).
        end_date (datetime): End date of the data range (inclusive up to 23:00:00).
        dataset (str): Name of the dataset (e.g., '2m_temperature').
        api_key (Optional[str]): CDS API key for Copernicus downloads, if needed.
        force_download (bool): If True, forces re-download of GRIB files on validation failure.

    Returns:
        xr.Dataset: Validated xarray Dataset with the correct number of hourly timestamps.

    Raises:
        ValueError: If input parameters are invalid (e.g., empty grib_paths, invalid dates).
        RuntimeError: If data validation fails after forced re-download.
    """
    appending = kwargs.get('appending', False)

    # Input validation
    if not grib_paths:
        raise ValueError("grib_paths cannot be empty")
    if not dataset:
        raise ValueError("dataset name cannot be empty")
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")

    # Ensure datetimes are normalized to midnight
    # Sort GRIB files by filename stem
    grib_paths.sort(key=lambda p: p.stem)

    # Open dataset
    ds = xr.open_mfdataset(grib_paths, engine='cfgrib', decode_timedelta=False)

    ds = standardize(dataset, ds)

    # Create time slice (inclusive up to end_date 23:00:00)
    # slice_end_date = end_date.replace(hour=23, minute=0, second=0)
    time_slice = slice(np.datetime64(start_date), np.datetime64(end_date))
    ds = ds.sel(time=time_slice)

    ds = ds.chunk(chunking_settings)
    ds.coords['time'].encoding['chunks'] = (time_chunk_size,)


    time_delta = np.datetime64(end_date) - np.datetime64(start_date)
    expected_hours = round(time_delta / np.timedelta64(1, 'h')) + 1
    actual_hours = ds.sizes.get('time', 0)
    eprint(f"Expected {expected_hours} hours, found {actual_hours}. From {start_date} to {end_date}.")

    expected_time_chunk = chunking_settings['time']
    actual_time_chunks = ds[dataset].sizes['time']

    if (actual_time_chunks != expected_time_chunk) and not appending:
        error_msg = (
            f"Chunking validation failed for 'time' dimension. "
            f"Expected one chunk of size {expected_time_chunk}, but found chunks: {actual_time_chunks}."
        )
        eprint(f"⚠️ {error_msg}")
        raise RuntimeError(error_msg)


    # Validate data integrity
    if actual_hours != expected_hours:
        eprint(f"⚠️ Data integrity check failed. Expected {expected_hours} hours, found {actual_hours}.")
        if not force_download:
            raise RuntimeError(
                f"Data integrity check failed: expected {expected_hours} hours, found {actual_hours}. "
                "Set force_download=True to attempt re-download."
            )

        eprint("Attempting forced re-download from source...")
        try:
            grib_paths = await get_gribs_for_date_range_async(
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
                api_key=api_key,
                force=True,
            )
        except Exception as e:
            raise RuntimeError(f"Forced re-download failed: {e}")

        # Re-open and re-slice dataset
        grib_paths.sort(key=lambda p: p.stem)
        ds = xr.open_mfdataset(grib_paths, engine='cfgrib', decode_timedelta=False)
        ds = ds.sel(time=time_slice)
        actual_hours = ds.sizes.get('time', 0)

        if actual_hours != expected_hours:
            raise RuntimeError(
                f"FATAL: Forced re-download did not resolve data corruption. "
                f"Expected {expected_hours} hours, found {actual_hours}. "
                "Source data may be incomplete for this period."
            )
        eprint("✅ Forced re-download successful. Data integrity confirmed.")
    else:
        eprint("✅ Data integrity confirmed on first attempt.")

    return ds