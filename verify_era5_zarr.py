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

# --- Global Cache for GRIB files ---
# This is a performance optimization. It avoids reloading the same monthly GRIB file
# from disk over and over again. The key will be the file path, the value will be the
# loaded xarray.Dataset object.
GRIB_CACHE = {}

chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


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
def main(cid: str, dataset_name: str, grib_dir: Path, num_checks: int):
    """
    Verifies a sharded ERA5 Zarr dataset on IPFS against local source GRIB files.

    It loads the Zarr dataset from the given CID, then in a loop, it picks
    random points in time and space and compares the value in the Zarr dataset
    with the value from the corresponding source GRIB file.

    \b
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
        print(f"Checks to perform: {num_checks}\n")

        match_count = 0
        mismatch_count = 0
        error_count = 0

        async with KuboCAS() as cas:
            try:
                # 1. Load the large Zarr dataset from IPFS
                print("⏳ Loading Zarr dataset from IPFS... (this may take a moment)")
                zarr_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=cid)
                zarr_ds = xr.open_zarr(zarr_store)
                print("✅ Zarr dataset loaded successfully.")
                print(f"   Dimensions: {zarr_ds.dims}\n")

                # Get coordinate arrays once to pick from later
                time_coords = zarr_ds.time.values
                lat_coords = zarr_ds.latitude.values
                lon_coords = zarr_ds.longitude.values

            except Exception as e:
                print(f"❌ FATAL: Could not load Zarr dataset from CID {cid}. Error: {e}", file=sys.stderr)
                sys.exit(1)

            # 2. Start the verification loop
            for i in range(num_checks):
                print(f"\n--- Check {i + 1}/{num_checks} ---")

                # 3. Pick a random point in time and space
                rand_time = random.choice(time_coords)
                rand_lat = random.choice(lat_coords)
                rand_lon = random.choice(lon_coords)
                
                print(f"📍 Random point selected:")
                print(f"   Time: {pd.to_datetime(rand_time)}")
                print(f"   Latitude: {rand_lat:.2f}, Longitude: {rand_lon:.2f}")

                try:
                    # 4. Get value from the Zarr dataset
                    print("   - Querying Zarr dataset...")
                    zarr_value = zarr_ds[dataset_name].sel(
                        time=rand_time,
                        latitude=rand_lat,
                        longitude=rand_lon
                    ).compute().item() # .item() extracts the single scalar value

                    # 5. Find and load the corresponding source GRIB file
                    grib_ds = find_and_load_grib(rand_time, dataset_name, grib_dir)
                    if grib_ds is None:
                        error_count += 1
                        continue # Skip to next check if GRIB file can't be loaded

                    # 6. Get value from the GRIB dataset
                    print("   - Querying source GRIB dataset...")
                    grib_value = grib_ds[dataset_name].sel(
                        time=rand_time,
                        latitude=rand_lat,
                        longitude=rand_lon,
                        method="nearest" # Use 'nearest' for GRIBs in case of float precision differences
                    ).compute().item()

                    # 7. Compare the values
                    print(f"   Zarr value: {zarr_value:.4f}")
                    print(f"   GRIB value: {grib_value:.4f}")

                    if np.isclose(zarr_value, grib_value, equal_nan=True):
                        print("   --> ✅ MATCH")
                        match_count += 1
                    else:
                        print(f"   --> ❌ MISMATCH! Difference: {zarr_value - grib_value:.6f}")
                        mismatch_count += 1

                except Exception as e:
                    print(f"   --> ❌ ERROR during check: {e}", file=sys.stderr)
                    error_count += 1
        
        # 8. Print final summary
        print("\n\n--- Verification Complete ---")
        print(f"Total points checked: {num_checks}")
        print(f"✅ Matches: {match_count}")
        print(f"❌ Mismatches: {mismatch_count}")
        print(f"⚙️ Errors (e.g., missing GRIBs): {error_count}")
        print("---------------------------")


    asyncio.run(run_verification())


if __name__ == "__main__":
    main()