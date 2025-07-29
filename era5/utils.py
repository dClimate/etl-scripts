# utils.py
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

dataset_names = [
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "surface_pressure",
    "surface_solar_radiation_downwards",
    "total_precipitation",
    "land_total_precipitation" # DOUBLE CHECK THIS
    "era5_volumetric_soil_water_layer_3" # DOUBLE CHECK THIS
]

start_dates = {
    "2m_temperature": datetime(1940, 1, 1),
    "total_precipitation": datetime(1940, 1, 1, 7, 0, 0),  # First data is from 1940-01-02
    "land_total_precipitation": datetime(1950, 1, 1, 0, 0, 0),  # First data is from 1950-01-01
}

era5_env: dict[str, str]
with open(Path(__file__).parent / "era5-env.json") as f:
    era5_env = json.load(f)

CHUNKER = "size-1048576"
chunking_settings = {"time": 5000, "latitude": 6, "longitude": 8}
time_chunk_size = 500000

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
            api_key = era5_env["CDS_API_KEY"]
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