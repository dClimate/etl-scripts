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


# --- Global Cache for GRIB files ---
# This is a performance optimization. It avoids reloading the same monthly GRIB file
# from disk over and over again. The key will be the file path, the value will be the
# loaded xarray.Dataset object.
GRIB_CACHE = {}

chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


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
    print(f"--- Checking Zarr Dataset for Zero/NaN Values at Lat={latitude}, Lon={longitude} ---")
    print(f"CID: {cid}")
    print(f"Dataset: {dataset_name}")
    if start_date:
        print(f"Start date: {start_date}")
    if end_date:
        print(f"End date: {end_date}")
    print("")

    async with KuboCAS() as cas:
        try:
            # Load Zarr dataset
            print("⏳ Loading Zarr dataset from IPFS...")
            start_time = time.time()
            zarr_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=cid)
            zarr_ds = xr.open_zarr(zarr_store, chunks='auto')  # Enable dask for parallel loading
            elapsed = time.time() - start_time
            print(f"✅ Zarr dataset loaded successfully in {elapsed:.2f} seconds.")
            print(f"   Dimensions: {zarr_ds.dims}")

            # Filter time coordinates
            time_coords = zarr_ds.time.values
            if start_date:
                try:
                    time_coords = time_coords[time_coords >= np.datetime64(start_date)]
                except ValueError as e:
                    print(f"❌ ERROR: Invalid start_date format '{start_date}'. Use YYYY-MM-DD. Error: {e}")
                    sys.exit(1)
            if end_date:
                try:
                    time_coords = time_coords[time_coords <= np.datetime64(end_date)]
                except ValueError as e:
                    print(f"❌ ERROR: Invalid end_date format '{end_date}'. Use YYYY-MM-DD. Error: {e}")
                    sys.exit(1)
            if len(time_coords) == 0:
                print(f"❌ ERROR: No time coordinates in range {start_date or 'start'} to {end_date or 'end'}")
                sys.exit(1)

            # Select data for the entire time dimension at the specified location
            print("\n⏳ Selecting and computing data for the entire time dimension...")
            start_time = time.time()

            print(time_coords, latitude, longitude)

            # Use dask for parallel computation
            data = zarr_ds[dataset_name].sel(
                time=time_coords,
                latitude=latitude,
                longitude=longitude,
                method="nearest"
            )
            print(data)
            
            # Compute with progress bar
            data_values = data.compute().values
            
            elapsed = time.time() - start_time
            print(f"✅ Data computed in {elapsed:.2f} seconds")

            # Check for zero or NaN values
            invalid_indices = np.where((data_values == 0.0) | np.isnan(data_values))[0]
            zero_or_nan_dates = [pd.to_datetime(time_coords[i]) for i in invalid_indices]

            # Summary
            print("\n--- Check Complete ---")
            print(f"Total time points checked: {len(time_coords)}")
            print(f"Total zero or NaN values found: {len(zero_or_nan_dates)}")
            if zero_or_nan_dates:
                print("Dates with zero or NaN values:")
                for zd in sorted(zero_or_nan_dates):  # Sort dates for clarity
                    print(f"  - {zd}")
                hours_since_1940 = (np.datetime64("2008-06-06") - np.datetime64("1940-01-01")) / np.timedelta64(1, 'h')
                expected_chunk_index = int(hours_since_1940 // chunking_settings["time"])
                print(f"\nExpected chunk index for 2008-06-06: {expected_chunk_index}")
                
                # Identify affected chunks
                time_chunks = zarr_ds[dataset_name].chunks[0]
                time_chunk_starts = np.cumsum([0] + list(time_chunks[:-1]))
                for zd in zero_or_nan_dates:
                    time_idx = np.where(time_coords == np.datetime64(zd))[0]
                    if len(time_idx) > 0:
                        time_idx = time_idx[0]
                        chunk_idx = np.searchsorted(time_chunk_starts, time_idx, side='right') - 1
                        print(f"  - {zd} is in chunk {chunk_idx + 1}")
            else:
                print("No zero or NaN values found.")

        except Exception as e:
            print(f"❌ FATAL: Could not load Zarr dataset from CID {cid}. Error: {e}")
            sys.exit(1)

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
        print("Processing multi-dimensional valid_time...")
        ds_stack = ds.stack(throwaway=("time", "step"))
        ds_linear = ds_stack.rename({"throwaway": "valid_time"})  # Rename stacked dim to valid_time
        ds = ds_linear.drop_vars(["throwaway"], errors="ignore")
        print("After handling multi-dimensional valid_time:")
        perint("Dimensions:", ds.dims)
        print("Coordinates:", ds.coords)

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
            print("Time Index already exists, not setting it again.")

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

    # Check the cache first
    if grib_filepath in GRIB_CACHE:
        return GRIB_CACHE[grib_filepath]

    # If not in cache, check if the file exists on disk
    if not grib_filepath.exists():
        print(f"⚠️ WARNING: Source GRIB file not found: {grib_filepath}", file=sys.stderr)
        return None

    # Load the GRIB file and standardize it
    try:
        print(f"⚙️ Loading and standardizing source GRIB: {grib_filepath.name}...")
        grib_ds = xr.open_dataset(grib_filepath, engine='cfgrib', decode_timedelta=False)
        standardized_grib_ds = standardize(dataset_name, grib_ds)
        
        # Store the loaded and processed dataset in the cache
        GRIB_CACHE[grib_filepath] = standardized_grib_ds
        return standardized_grib_ds
    except Exception as e:
        print(f"❌ ERROR: Could not load or process GRIB file {grib_filepath}: {e}", file=sys.stderr)
        return None


@click.command()
@click.argument("cid")
@click.argument("dataset_name")
@click.argument("grib_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--num-checks", default=1000, show_default=True, help="Number of random points to verify.")
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date for time range (YYYY-MM-DD).")
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), help="End date for time range (YYYY-MM-DD).")
@click.option("--lat-min", type=float, default=-90, show_default=True, help="Minimum latitude for sampling.")
@click.option("--lat-max", type=float, default=90, show_default=True, help="Maximum latitude for sampling.")
@click.option("--lon-min", type=float, default=-180, show_default=True, help="Minimum longitude for sampling.")
@click.option("--lon-max", type=float, default=180, show_default=True, help="Maximum longitude for sampling.")
def main(cid: str, dataset_name: str, grib_dir: Path, num_checks: int, start_date, end_date, lat_min: float, lat_max: float, lon_min: float, lon_max: float):
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
    async def run_verification():
        print("--- ERA5 Zarr Verification Script ---")
        print(f"CID: {cid}")
        print(f"Dataset: {dataset_name}")
        print(f"Source GRIBs: {grib_dir}")
        print(f"Checks to perform: {num_checks}")
        print(f"Time range: {start_date or 'All'} to {end_date or 'All'}")
        print(f"Spatial bounds: Lat [{lat_min}, {lat_max}], Lon [{lon_min}, {lon_max}]\n")

        match_count = 0
        mismatch_count = 0
        error_count = 0

        await check_zeros_at_location(cid, dataset_name, lat_min, lon_min, start_date, end_date)

        async with KuboCAS() as cas:
            try:
                print("⏳ Loading Zarr dataset from IPFS... (this may take a moment)")
                zarr_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=cid)
                zarr_ds = xr.open_zarr(zarr_store)
                print("✅ Zarr dataset loaded successfully.")
                print(f"   Dimensions: {zarr_ds.dims}\n")

                # Filter coordinates based on input options
                time_coords = zarr_ds.time.values
                if start_date:
                    time_coords = time_coords[time_coords >= np.datetime64(start_date)]
                if end_date:
                    time_coords = time_coords[time_coords <= np.datetime64(end_date)]
                if len(time_coords) == 0:
                    print(f"❌ FATAL: No time coordinates available in the specified range {start_date} to {end_date}", file=sys.stderr)
                    sys.exit(1)

                lat_coords = zarr_ds.latitude.values
                lat_coords = lat_coords[(lat_coords >= lat_min) & (lat_coords <= lat_max)]
                lon_coords = zarr_ds.longitude.values
                lon_coords = lon_coords[(lon_coords >= lon_min) & (lon_coords <= lon_max)]
                if len(lat_coords) == 0 or len(lon_coords) == 0:
                    print(f"❌ FATAL: No coordinates available in the specified spatial bounds Lat [{lat_min}, {lat_max}], Lon [{lon_min}, {lon_max}]", file=sys.stderr)
                    sys.exit(1)

            except Exception as e:
                print(f"❌ FATAL: Could not load Zarr dataset from CID {cid}. Error: {e}", file=sys.stderr)
                sys.exit(1)

            for i in range(num_checks):
                print(f"\n--- Check {i + 1}/{num_checks} ---")

                rand_time = random.choice(time_coords)
                rand_lat = random.choice(lat_coords)
                rand_lon = random.choice(lon_coords)
                
                print(f"📍 Random point selected:")
                print(f"   Time: {pd.to_datetime(rand_time)}")
                print(f"   Latitude: {rand_lat:.2f}, Longitude: {rand_lon:.2f}")

                try:
                    print("   - Querying Zarr dataset...")
                    zarr_value = zarr_ds[dataset_name].sel(
                        time=rand_time,
                        latitude=rand_lat,
                        longitude=rand_lon,
                        method="nearest"  # Added to handle potential coordinate precision issues
                    ).compute().item()
                    print(f"   Zarr value: {zarr_value:.4f}")

                    grib_ds = find_and_load_grib(rand_time, dataset_name, grib_dir)
                    if grib_ds is None:
                        error_count += 1
                        continue

                    print("   - Querying source GRIB dataset...")
                    grib_value = grib_ds[dataset_name].sel(
                        time=rand_time,
                        latitude=rand_lat,
                        longitude=rand_lon,
                        method="nearest"
                    ).compute().item()
                    print(f"   GRIB value: {grib_value:.4f}")

                    if np.isclose(zarr_value, grib_value, rtol=1e-3, atol=1e-3, equal_nan=True):
                        print("   --> ✅ MATCH")
                        match_count += 1
                    else:
                        print(f"   --> ❌ MISMATCH! Difference: {zarr_value - grib_value:.6f}")
                        zarr_region = zarr_ds[dataset_name].sel(
                            time=rand_time,
                            latitude=slice(rand_lat - 0.5, rand_lat + 0.5),
                            longitude=slice(rand_lon - 0.5, rand_lon + 0.5),
                        ).compute()
                        print("   Zarr nearby values:", zarr_region.values)
                        mismatch_count += 1

                except Exception as e:
                    print(f"   --> ❌ ERROR during check: {e}", file=sys.stderr)
                    error_count += 1
        
        print("\n\n--- Verification Complete ---")
        print(f"Total points checked: {num_checks}")
        print(f"✅ Matches: {match_count}")
        print(f"❌ Mismatches: {mismatch_count}")
        print(f"⚙️ Errors (e.g., missing GRIBs): {error_count}")
        print("---------------------------")

    asyncio.run(run_verification())

if __name__ == "__main__":
    main()