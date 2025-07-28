# verifier.py
import asyncio
import random
from pathlib import Path
import sys

import click
import numpy as np
import xarray as xr
from py_hamt import KuboCAS, ShardedZarrStore
import pandas as pd
import warnings
import zarr
import time
from era5.standardizer import standardize
from etl_scripts.grabbag import eprint, npdt_to_pydt
from era5.utils import CHUNKER, dataset_names, chunking_settings, time_chunk_size

scratchspace: Path = (Path(__file__).parent/ "scratchspace").absolute()

async def check_zeros_at_location(
    cid: str,
    dataset_name: str,
    latitude: float,
    longitude: float,
    start_date: str | None,
    end_date: str | None
):
    """
    Checks a single location in the Zarr dataset for zero or NaN values across the entire time dimension.
    Uses xarray and dask for parallel data loading and computation.
    Reports dates where the value is zero or NaN, indicating potential grafting issues.
    
    Args:
        cid: Root CID of the sharded Zarr dataset.
        dataset_name: Name of the dataset (e.g., '2m_temperature').
        latitude: Latitude to check.
        longitude: Longitude to check.
        start_date: Optional start date (YYYY-MM-DD) to limit check range.
        end_date: Optional end date (YYYY-MM-DD) to limit check range.
    """
    eprint(f"--- Checking Zarr Dataset for Zero/NaN Values at Lat={latitude}, Lon={longitude} ---")
    eprint(f"CID: {cid}")
    eprint(f"Dataset: {dataset_name}")
    if start_date:
        eprint(f"Start date: {start_date}")
    if end_date:
        eprint(f"End date: {end_date}")

    async with KuboCAS() as cas:
        try:
            # Load Zarr dataset
            eprint("⏳ Loading Zarr dataset from IPFS...")
            start_time = time.time()
            zarr_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=cid)
            zarr_ds = xr.open_zarr(zarr_store, chunks='auto')  # Enable dask for parallel loading
            elapsed = time.time() - start_time
            eprint(f"✅ Zarr dataset loaded successfully in {elapsed:.2f} seconds.")
            eprint(f"   Dimensions: {zarr_ds.dims}")

            # Filter time coordinates
            time_coords = zarr_ds.time.values
            if start_date:
                try:
                    time_coords = time_coords[time_coords >= np.datetime64(start_date)]
                except ValueError as e:
                    eprint(f"❌ ERROR: Invalid start_date format '{start_date}'. Use YYYY-MM-DD. Error: {e}")
                    sys.exit(1)
            if end_date:
                try:
                    time_coords = time_coords[time_coords <= np.datetime64(end_date)]
                except ValueError as e:
                    eprint(f"❌ ERROR: Invalid end_date format '{end_date}'. Use YYYY-MM-DD. Error: {e}")
                    sys.exit(1)
            if len(time_coords) == 0:
                eprint(f"❌ ERROR: No time coordinates in range {start_date or 'start'} to {end_date or 'end'}")
                sys.exit(1)

            # Select data for the entire time dimension at the specified location
            eprint("\n⏳ Selecting and computing data for the entire time dimension...")
            start_time = time.time()

            eprint(time_coords, latitude, longitude)

            # Use dask for parallel computation
            data = zarr_ds[dataset_name].sel(
                time=time_coords,
                latitude=latitude,
                longitude=longitude,
                method="nearest"
            )
            eprint(data)
            
            # Compute with progress bar
            data_values = data.compute().values
            
            elapsed = time.time() - start_time
            eprint(f"✅ Data computed in {elapsed:.2f} seconds")

            # Check for zero or NaN values
            invalid_indices = np.where((data_values == 0.0) | np.isnan(data_values))[0]
            zero_or_nan_dates = [pd.to_datetime(time_coords[i]) for i in invalid_indices]

            # Summary
            eprint("\n--- Check Complete ---")
            eprint(f"Total time points checked: {len(time_coords)}")
            eprint(f"Total zero or NaN values found: {len(zero_or_nan_dates)}")
            if zero_or_nan_dates:
                eprint("Dates with zero or NaN values:")
                for zd in sorted(zero_or_nan_dates):  # Sort dates for clarity
                    eprint(f"  - {zd}")
                hours_since_1940 = (np.datetime64("2008-06-06") - np.datetime64("1940-01-01")) / np.timedelta64(1, 'h')
                expected_chunk_index = int(hours_since_1940 // chunking_settings["time"])
                eprint(f"\nExpected chunk index for 2008-06-06: {expected_chunk_index}")
                
                # Identify affected chunks
                time_chunks = zarr_ds[dataset_name].chunks[0]
                time_chunk_starts = np.cumsum([0] + list(time_chunks[:-1]))
                for zd in zero_or_nan_dates:
                    time_idx = np.where(time_coords == np.datetime64(zd))[0]
                    if len(time_idx) > 0:
                        time_idx = time_idx[0]
                        chunk_idx = np.searchsorted(time_chunk_starts, time_idx, side='right') - 1
                        eprint(f"  - {zd} is in chunk {chunk_idx + 1}")
            else:
                eprint("No zero or NaN values found.")

        except Exception as e:
            eprint(f"❌ FATAL: Could not load Zarr dataset from CID {cid}. Error: {e}")
            sys.exit(1)

def find_and_load_grib(timestamp: np.datetime64, dataset_name: str, grib_dir: Path) -> xr.Dataset | None:
    """
    Finds the correct monthly GRIB file for a given timestamp, loads it,
    and returns the standardized xarray.Dataset. Uses a global cache.
    """
    # Convert numpy.datetime64 to a standard Python datetime object
    dt_object = pd.to_datetime(timestamp).to_pydatetime()
    
    # Construct the expected monthly GRIB filename (e.g., '2m_temperature-202503.grib')
    grib_filename = f"{dataset_name}-{dt_object.strftime('%Y%m')}.grib"
    grib_filepath = grib_dir / grib_filename

    # If not in cache, check if the file exists on disk
    if not grib_filepath.exists():
        eprint(f"⚠️ WARNING: Source GRIB file not found: {grib_filepath}", file=sys.stderr)
        return None

    # Load the GRIB file and standardize it
    try:
        grib_ds = xr.open_dataset(grib_filepath, engine='cfgrib', decode_timedelta=False)
        standardized_grib_ds = standardize(dataset_name, grib_ds)
        
        return standardized_grib_ds
    except Exception as e:
        eprint(f"❌ ERROR: Could not load or process GRIB file {grib_filepath}: {e}", file=sys.stderr)
        return None

def load_grib_range(start_date: str, end_date: str, dataset_name: str, grib_dir: Path) -> xr.Dataset:
    """
    Finds and loads all monthly GRIB files within a date range into a single
    standardized xarray.Dataset.
    """
    eprint(f"⚙️ Finding GRIB files for range {start_date} to {end_date}...")
    normalized_start_date = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months_needed = pd.date_range(start=normalized_start_date, end=end_date, freq='MS')

    grib_files = []
    for dt in months_needed:
        # Construct the expected monthly GRIB filename (e.g., '2m_temperature-202503.grib')
        grib_filename = f"{dataset_name}-{dt.strftime('%Y%m')}.grib"
        eprint(f"Looking for {grib_filename}")
        grib_filepath = grib_dir / grib_filename
        if grib_filepath.exists():
            grib_files.append(grib_filepath)
        else:
            eprint(f"⚠️ WARNING: Source GRIB file not found, skipping: {grib_filepath}")

    if not grib_files:
        raise FileNotFoundError("Could not find any source GRIB files for the specified date range.")

    eprint(f"   --> Found {len(grib_files)} files. Loading with xr.open_mfdataset...")
    grib_ds = xr.open_mfdataset(grib_files, engine='cfgrib', decode_timedelta=False)

    # Standardize the entire combined dataset once
    return standardize(dataset_name, grib_ds)

async def run_checks(cid: str, dataset_name: str, num_checks: int, start_date, end_date):
    """
    Verifies a sharded ERA5 Zarr dataset on IPFS against local source GRIB files.

    It loads the Zarr dataset from the given CID, then in a loop, it picks
    random points in time and space (within the specified date range and lat/lon bounds)
    and compares the value in the Zarr dataset with the value from the corresponding source GRIB file.

    Arguments:
      CID: The root CID of the sharded Zarr dataset to verify.
      DATASET_NAME: The name of the dataset (e.g., '2m_temperature').
      GRIB_DIR: The path to the directory containing your monthly source GRIB files.
    """
    eprint("--- ERA5 Zarr Verification Script ---")
    eprint(f"CID: {cid}")
    eprint(f"Dataset: {dataset_name}")
    eprint(f"Checks to perform: {num_checks}")
    eprint(f"Time range: {start_date or 'All'} to {end_date or 'All'}")
    # eprint(f"Spatial bounds: Lat [{lat_min}, {lat_max}], Lon [{lon_min}, {lon_max}]\n")

    grib_dir: Path = scratchspace / dataset_name

    match_count = 0
    mismatch_count = 0
    error_count = 0

    # await check_zeros_at_location(cid, dataset_name, start_date, end_date)

    async with KuboCAS() as cas:
        try:
            eprint("⏳ Loading Zarr dataset from IPFS... (this may take a moment)")
            zarr_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=cid)
            zarr_ds = xr.open_zarr(zarr_store)
            eprint("✅ Zarr dataset loaded successfully.")
            eprint(f"   Dimensions: {zarr_ds.dims}\n")

            # Filter coordinates based on input options
            time_coords = zarr_ds.time.values
            if start_date:
                time_coords = time_coords[time_coords >= np.datetime64(start_date)]
            if end_date:
                time_coords = time_coords[time_coords <= np.datetime64(end_date)]
            if len(time_coords) == 0:
                eprint(f"❌ FATAL: No time coordinates available in the specified range {start_date} to {end_date}", file=sys.stderr)
                sys.exit(1)

            lat_coords = zarr_ds.latitude.values
            lon_coords = zarr_ds.longitude.values

        except Exception as e:
            eprint(f"❌ FATAL: Could not load Zarr dataset from CID {cid}. Error: {e}", file=sys.stderr)
            sys.exit(1)
        
        for i in range(num_checks):
            rand_time = random.choice(time_coords)
            rand_lat = random.choice(lat_coords)
            rand_lon = random.choice(lon_coords)
            try:
                zarr_value = zarr_ds[dataset_name].sel(
                    time=rand_time,
                    latitude=rand_lat,
                    longitude=rand_lon,
                    method="nearest"  # Added to handle potential coordinate precision issues
                ).compute().item()
        
                grib_ds = find_and_load_grib(rand_time, dataset_name, grib_dir)
                if grib_ds is None:
                     raise ValueError("Could not load grib")

                grib_value = grib_ds[dataset_name].sel(
                    time=rand_time,
                    latitude=rand_lat,
                    longitude=rand_lon,
                    method="nearest"
                ).compute().item()

                if np.equal(zarr_value, grib_value):
                    match_count += 1
                    if (match_count % 10 == 0):
                        eprint(f"✅ {match_count} Points Match")
                else:
                    difference = zarr_value - grib_value
                    error_message = (
                        f"❌ Data validation failed: Mismatch found between Zarr and GRIB data.\n"
                        f"------------------------------------------------------------------\n"
                        f"Coordinates:\n"
                        f"  - Time: {rand_time}\n"
                        f"  - Latitude: {rand_lat}\n"
                        f"  - Longitude: {rand_lon}\n"
                        f"\n"
                        f"Values:\n"
                        f"  - GRIB Value: {grib_value:.6f}\n"
                        f"  - Zarr Value: {zarr_value:.6f}\n"
                        f"  - Difference: {difference:.6f}\n"
                        f"\n"
                        f"------------------------------------------------------------------"
                    )
                    print("ERROR FOUND", error_message)
                    
                    # Raise an exception to halt execution
                    # raise ValueError(error_message)

            except Exception as e:
                raise
    
    eprint("\n\n--- Verification Complete ---")
    eprint(f"Total points checked: {num_checks}")
    eprint("---------------------------")

async def compare_datasets(cid: str, dataset_name: str, start_date, end_date, lat_min: float, lat_max: float, lon_min: float, lon_max: float):
    """
    Verifies a sharded ERA5 Zarr dataset against local source GRIB files
    by comparing all data points within the specified bounds.
    """
    # Adjust end_date to ensure it includes the last hour of the day

    eprint("--- ERA5 Zarr Full Verification ---")
    eprint(f"CID: {cid}")
    eprint(f"Dataset: {dataset_name}")
    eprint(f"Time range: {start_date} to {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    eprint(f"Spatial bounds: Lat [{lat_min}, {lat_max}], Lon [{lon_min}, {lon_max}]\n")

    grib_dir: Path = scratchspace

    async with KuboCAS() as cas:
        try:
            # 1. Load Zarr data slice from IPFS
            eprint("⏳ Loading Zarr dataset from IPFS...")
            zarr_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=cid)
            zarr_ds = xr.open_zarr(zarr_store)

            is_single_point = (start_date == end_date) and (lat_min == lat_max) and (lon_min == lon_max)
            grib_ds = load_grib_range(start_date, end_date, dataset_name, grib_dir)

            if is_single_point:
                eprint(f"Single point verification at Lat: {lat_min}, Lon: {lon_min}, Date: {start_date}")
                zarr_slice = zarr_ds.sel(time=start_date, latitude=lat_min, longitude=lon_min, method='nearest').load()
                grib_slice = grib_ds.sel(time=start_date, latitude=lat_min, longitude=lon_min, method='nearest').load()
            else:
                eprint("Loading Zarr data slice for the specified bounds...")
                zarr_slice = zarr_ds.sel(
                    time=slice(start_date, end_date),
                    latitude=slice(lat_min, lat_max),
                    longitude=slice(lon_min, lon_max),
                ).load()
                eprint("✅ Zarr data slice loaded successfully.")
                if zarr_slice.nbytes == 0:
                    eprint(f"❌ FATAL: Zarr slice is empty for the specified bounds.")
                    sys.exit(1)
                eprint("⏳ Loading source GRIB data for the entire range...")
                grib_slice = grib_ds.sel(
                    time=slice(start_date, end_date),
                    latitude=slice(lat_min, lat_max),
                    longitude=slice(lon_min, lon_max),
                ).load()
                eprint("✅ GRIB data slice loaded successfully.")

            # 3. Compare the two data arrays
            eprint("⏳ Comparing Zarr and GRIB data arrays...")

            # Assumes the `standardize` function in `load_grib_range` handles
            # coordinate sorting and dimension ordering (e.g., via transpose).
            xr.testing.assert_equal(zarr_slice[dataset_name], grib_slice[dataset_name])
            
            eprint("✅ SUCCESS: All values in the Zarr dataset match the source GRIB files within the tolerance.")

        except FileNotFoundError as e:
            eprint(f"❌ VALIDATION FAILED: Missing source data. Error: {e}")
            sys.exit(1)
        except Exception as e:
            eprint(f"❌ VALIDATION FAILED: An error occurred during verification.")
            eprint(f"\n--- Error Details ---\n{e}\n---------------------\n")
            debug_xarray_differences(zarr_slice[dataset_name], grib_slice[dataset_name], name1="Zarr", name2="GRIB")
            sys.exit(1)

    eprint("\n--- Verification Complete ---")

def debug_xarray_differences(da1: xr.DataArray, da2: xr.DataArray, name1="L", name2="R"):
    """
    Performs a deep comparison of two xarray DataArrays and prints a detailed diagnostic report.

    Args:
        da1 (xr.DataArray): The first array (e.g., from Zarr).
        da2 (xr.DataArray): The second array (e.g., from GRIB).
        name1 (str): Name for the first array.
        name2 (str): Name for the second array.
    """
    eprint(f"--- Starting Deep Comparison between '{name1}' and '{name2}' ---")
    has_issues = False

    # 1. Metadata Comparison
    eprint("\n## 1. Metadata Comparison")
    # Check Shape
    if da1.shape != da2.shape:
        eprint(f"❌ SHAPE MISMATCH: '{name1}' has {da1.shape}, '{name2}' has {da2.shape}")
        has_issues = True
    else:
        eprint(f"✅ Shapes are identical: {da1.shape}")

    # Check Data Type
    if da1.dtype != da2.dtype:
        eprint(f"❌ DTYPE MISMATCH: '{name1}' has {da1.dtype}, '{name2}' has {da2.dtype}")
        has_issues = True
    else:
        eprint(f"✅ Dtypes are identical: {da1.dtype}")

    # Check Attributes
    if da1.attrs != da2.attrs:
        eprint(f"⚠️  Attributes are different.")
        # You can add more detailed attribute comparison here if needed
        has_issues = True
    else:
        eprint(f"✅ Attributes are identical.")
        
    eprint("-" * 25)

    # 2. Value Comparison
    eprint("\n## 2. Value Comparison")


    # Find where one is NaN and the other is not. This is a common issue.
    nan_mismatch_mask = np.isnan(da1) != np.isnan(da2)
    nan_mismatch_count = nan_mismatch_mask.sum().item()

    # Find purely numeric differences (where both values are numbers)
    # Using np.isclose is better than direct comparison for floats
    numeric_mismatch_mask = ~np.isclose(da1, da2, equal_nan=True)
    # Exclude the NaN mismatches we already found
    numeric_mismatch_mask = numeric_mismatch_mask & ~nan_mismatch_mask
    numeric_mismatch_count = numeric_mismatch_mask.sum().item()
    
    total_mismatches = nan_mismatch_count + numeric_mismatch_count

    if total_mismatches == 0:
        eprint("✅ All values are numerically identical.")
    else:
        eprint(f"❌ Found {total_mismatches} mismatched value(s).")
        has_issues = True
        
        # 3. Detailed Mismatch Report
        eprint("\n## 3. Detailed Mismatch Report")

        if nan_mismatch_count > 0:
            eprint(f"  - Found {nan_mismatch_count} locations with NaN mismatches.")
            # Find the first example of a NaN mismatch
            example_coords = nan_mismatch_mask.where(nan_mismatch_mask, drop=True).isel({dim: 0 for dim in nan_mismatch_mask.dims}).coords
            val1 = da1.sel(example_coords).item()
            val2 = da2.sel(example_coords).item()
            eprint(f"    - Example at {dict(example_coords)}:")
            eprint(f"      - '{name1}' value: {val1}")
            eprint(f"      - '{name2}' value: {val2}")

        if numeric_mismatch_count > 0:
            diff = np.abs(da1 - da2)
            max_diff = diff.where(numeric_mismatch_mask).max().item()
            eprint(f"  - Found {numeric_mismatch_count} numeric mismatches.")
            eprint(f"    - Maximum absolute difference: {max_diff:.10f}")
            
            # Find location of the largest difference
            max_diff_coords = diff.where(diff == max_diff, drop=True).isel({dim: 0 for dim in diff.dims}).coords
            val1 = da1.sel(max_diff_coords).item()
            val2 = da2.sel(max_diff_coords).item()
            eprint(f"    - Location of max difference: {dict(max_diff_coords)}")
            eprint(f"      - '{name1}' value: {val1:.10f}")
            eprint(f"      - '{name2}' value: {val2:.10f}")

    eprint("\n--- Deep Comparison Finished ---")
    if not has_issues:
        eprint("✅ SUCCESS: Arrays appear to be fully identical.")