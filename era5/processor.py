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

from era5.utils import get_latest_timestamp
from era5.downloader import get_gribs_for_date_range_async
from era5.validator import validate_data
from era5.standardizer import standardize
from era5.verifier import compare_datasets, run_checks
from era5.utils import CHUNKER, dataset_names, chunking_settings, time_chunk_size


scratchspace: Path = (Path(__file__).parent.parent / "scratchspace" / "era5").absolute()
os.makedirs(scratchspace, exist_ok=True)

era5_env: dict[str, str]
with open(Path(__file__).parent / "era5-env.json") as f:
    era5_env = json.load(f)

datasets_choice = click.Choice(dataset_names)
period_options = ["hour", "day", "month"]
period_choice = click.Choice(period_options)

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

async def chunked_write(ds: xr.Dataset, variable_name: str, rpc_uri_stem, gateway_uri_stem) -> str:
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem, chunker=CHUNKER) as kubo_cas:
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
            chunks_per_shard=6250,
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
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem, chunker=CHUNKER) as kubo_cas:
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
            np.datetime64(end_date.replace(hour=23, minute=0, second=0)) + np.timedelta64(1, 'h'),
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

async def check_for_cid(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
):

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
            return existing_cid
            expected_hours = ((end_date - start_date).days + 1) * 24
            is_valid = False
            try:
                async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem, chunker=CHUNKER) as kubo_cas:
                    store = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=existing_cid)
                    ds = xr.open_zarr(store)
                    if ds.sizes.get('time', 0) == expected_hours:
                        is_valid = True
                    else:
                            eprint(f"Cached CID is invalid. Expected {expected_hours} hours, but found {ds.sizes.get('time', 0)}.")
            except Exception as e:
                eprint(f"⚠️  Validation failed for cached CID {existing_cid}: {e}")
                is_valid = False

            if is_valid:
                eprint("✅ Cached CID is valid. Skipping processing.")
                return existing_cid
            else:
                eprint("Proceeding with fresh download and processing.")
    return None

async def batch_processor(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
    initial: bool = False,
):
    """
    Downloads, processes, and creates an IPFS Zarr store for a single batch of data.
    On success, prints the final CID to standard output. All logging goes to stderr.
    This command is intended to be called as a subprocess by the 'append' command.
    """
    try:
        cid_found = await check_for_cid(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            gateway_uri_stem=gateway_uri_stem,
            rpc_uri_stem=rpc_uri_stem,
        )
        if cid_found:
            return cid_found
        eprint(f"--- Starting batch process for {dataset} from {start_date.date()} to {end_date.date()} ---")
        ds: xr.Dataset | None = None
        # 1. INITIAL DOWNLOAD
        eprint("Attempting to fetch GRIBs from cache or source...")
        grib_paths = await get_gribs_for_date_range_async(
            dataset, start_date, end_date, api_key=api_key, force=False
        )
        if not grib_paths:
            eprint("No GRIB files were downloaded, exiting.")
            sys.exit(1)

        # 2. Validate the input data to ensure it gets what it expects
        ds = await validate_data(grib_paths, start_date, end_date, dataset, api_key)

        # 3. Standardize the data
        ds = standardize(dataset, ds)

        if (initial): 
            eprint(f"Forcing encoding with chunk settings: {chunking_settings}")
            encoding_chunks = tuple(chunking_settings.get(dim) for dim in ds[dataset].dims)
            ds[dataset].encoding['chunks'] = encoding_chunks
            for coord_name, coord_array in ds.coords.items():
                if coord_name in chunking_settings:
                    if coord_name == "time":
                        ds[coord_name].encoding['chunks'] = (time_chunk_size,)
                    else:
                        ds[coord_name].encoding['chunks'] = (ds.sizes[coord_name],)

            ordered_dims = list(ds.dims)
            array_shape = tuple(ds.sizes[dim] for dim in ordered_dims)
            chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)
            if ordered_dims != ["time", "latitude", "longitude"]:
                ordered_dims = ["time", "latitude", "longitude"]
                ds = ds.transpose(*ordered_dims)
                array_shape = tuple(ds.sizes[dim] for dim in ordered_dims)
                chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)

        # 4. Write to the store
        batch_cid = await chunked_write(ds, dataset, rpc_uri_stem=rpc_uri_stem, gateway_uri_stem=gateway_uri_stem)

        eprint("Uploaded to IPFS")
        eprint(batch_cid)

        # 5. Now Verify the data being written matches
        lat_min = ds.latitude.values[0]
        lat_max = ds.latitude.values[-1]
        lon_min = ds.longitude.values[0]
        lon_max = ds.longitude.values[-1]
        await compare_datasets(
            cid=batch_cid, 
            dataset_name=dataset, 
            start_date=start_date, 
            end_date=end_date, 
            lat_min=lat_min, 
            lat_max=lat_max, 
            lon_min=lon_min, 
            lon_max=lon_max,
        )

        # BACKUP CID
        save_cid_to_file(batch_cid, dataset, start_date, end_date, "batch")

        eprint(f"--- Batch process finished successfully for {start_date.date()} to {end_date.date()} ---")
        
        #eprint("Cleaning up GRIB files...")
        # for path in grib_paths:
            #     try:
            #         os.remove(path)
            #     except OSError as e:
            #         eprint(f"Warning: Could not remove GRIB file {path}: {e}")
        
        return batch_cid
    except Exception as e:
        eprint(f"ERROR: An error occurred during Zarr creation/upload: {e}")
        sys.exit(1)
    
async def append_latest(
    dataset: str,
    cid: str,
    end_date: datetime | None,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    api_key: str | None,
):
    """
    Appends all available new data from the dataset's last timestamp to the latest 
    available date in a single operation.
    """
    # 1. Determine the full date range for the append operation.
    if end_date is None:
        latest_available_date = get_latest_timestamp(dataset, api_key=api_key)
    else:
        latest_available_date = end_date

    target_end_date = (latest_available_date - timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
    print(target_end_date)
    
    final_cid = cid

    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem, chunker=CHUNKER) as kubo_cas:

        # 2. Open the store to find the last existing timestamp.
        initial_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=cid)
        initial_ds = xr.open_zarr(initial_store)
        
        last_timestamp = npdt_to_pydt(initial_ds.time[-1].values)
        start_date = last_timestamp + timedelta(hours=1)
        initial_ds.close()

        # Exit early if the dataset is already up-to-date.
        if target_end_date.date() <= last_timestamp.date():
            eprint(f"✅ Dataset is already up-to-date. Last timestamp: {last_timestamp.date()}.")
            print(cid)
            return cid

        eprint(f"--- Appending all data from {start_date.date()} to {target_end_date.date()} in a single operation ---")

        try:
            # 3. Get the processed xarray.Dataset for the *entire* date range.
            # Always force to make sure we have the latest
            grib_paths = await get_gribs_for_date_range_async(
                dataset, start_date, target_end_date, api_key=api_key, force=False
            )
            if not grib_paths:
                raise FileNotFoundError("No GRIB files were found or downloaded for the period.")

            eprint("Validating downloaded data...")
            ds = await validate_data(grib_paths, start_date, target_end_date, dataset, api_key)

            eprint("Standardizing dataset...")
            ds = standardize(dataset, ds)

            # 4. Open the main Zarr store for writing.
            main_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=False, root_cid=cid)
            
            # 5. Append all the new data in one call.
            eprint("Appending data to the Zarr store...")
            ds.to_zarr(main_store, mode='a', append_dim="time")
            
            # 6. Flush the store to commit changes and get the final CID.
            eprint("Flushing store to get new root CID...")
            new_cid_obj = await main_store.flush()
            final_cid = str(new_cid_obj)

            # 5. Now Verify the data being written matches
            lat_min = ds.latitude.values[0]
            lat_max = ds.latitude.values[-1]
            lon_min = ds.longitude.values[0]
            lon_max = ds.longitude.values[-1]
            await compare_datasets(
                cid=final_cid, 
                dataset_name=dataset, 
                start_date=start_date, 
                end_date=target_end_date, 
                lat_min=lat_min, 
                lat_max=lat_max, 
                lon_min=lon_min, 
                lon_max=lon_max,
            )

            await run_checks(cid=final_cid, dataset_name=dataset, num_checks=100, start_date=start_date, end_date=target_end_date)
            
        except Exception as e:
            eprint(f"❌ ERROR: Failed to process or append data. Error: {e}")
            sys.exit(1)
    
    eprint(f"\n✅ Append operation complete! Final CID: {final_cid}")
    print(final_cid)
    
    save_cid_to_file(final_cid, dataset, start_date, target_end_date, "append-direct")
    
    return final_cid


##
async def build_full_dataset(
    dataset: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
    max_parallel_procs: int,
):
    """
    Creates a new, chunk-aligned Zarr store on IPFS with the first 1200 hours (50 days) of ERA5 data,
    then extends it to the latest available timestamp using batch processing. Prints the final CID to stdout.
    All logging goes to stderr.
    """
    HOURS_PER_BATCH = 1200  # LCM of 24h (data unit) and 400h (Zarr chunk size)
    DAYS_PER_BATCH = HOURS_PER_BATCH // 24
    start_date = datetime(1940, 1, 1)
    end_date = get_latest_timestamp(dataset, api_key=api_key)


    delta_days = (end_date - start_date).days
    batches = delta_days // DAYS_PER_BATCH  # Round down
    adjusted_days = batches * DAYS_PER_BATCH - 1
    end_date = start_date + timedelta(days=adjusted_days)
    eprint(f"Building full dataset for {dataset} from {start_date.date()} to {end_date.date()}")

    # --- 1. Initialize Store with First 1200 Hours ---
    initial_end_date = start_date + timedelta(days=DAYS_PER_BATCH - 1)

    initial_cid = await batch_processor(
        dataset=dataset, 
        start_date=start_date, 
        end_date=initial_end_date, 
        gateway_uri_stem=gateway_uri_stem, 
        rpc_uri_stem=rpc_uri_stem, 
        api_key=api_key,
        initial=True,
    )

    # Open initial store to get last timestamp
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem, chunker=CHUNKER) as kubo_cas:
        initial_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=initial_cid)
        initial_ds = xr.open_zarr(initial_store)
        start_timestamp = npdt_to_pydt(initial_ds.time[-1].values)
        initial_shape = initial_store._array_shape
        initial_chunks_per_dim = initial_store._chunks_per_dim
        running_chunk_offset = initial_chunks_per_dim[0]
        initial_ds.close()

    eprint(f"Starting append from {start_timestamp.date()}. Target end date: {end_date.date()}")

    all_days_to_download = []
    current_ts = start_timestamp
    while current_ts < end_date:
        current_ts += timedelta(days=1)
        all_days_to_download.append(current_ts)

    if not all_days_to_download:
        eprint("No new days to download.")
        print(initial_cid)
        return

    batches_of_days = [
        all_days_to_download[i:i + DAYS_PER_BATCH]
        for i in range(0, len(all_days_to_download), DAYS_PER_BATCH)
        if len(all_days_to_download[i:i + DAYS_PER_BATCH]) == DAYS_PER_BATCH
    ]

    eprint(f"Planned {len(batches_of_days)} batches of up to {DAYS_PER_BATCH} days each.")

    # Extend the dataset skeleton
    extended_cid = await extend(dataset, initial_cid, end_date, gateway_uri_stem, rpc_uri_stem)
    if not extended_cid:
        eprint("Failed to extend dataset.")
        sys.exit(1)

    # Process batches in parallel
    processes = []
    batch_info = []
    for i, batch in enumerate(batches_of_days):
        batch_start_date = batch[0]
        batch_end_date = batch[-1]

        cid_found = await check_for_cid(
            dataset=dataset,
            start_date=batch_start_date,
            end_date=batch_end_date,
            gateway_uri_stem=gateway_uri_stem,
            rpc_uri_stem=rpc_uri_stem,
        )
        
        if cid_found:
            eprint(f"--> Found existing CID {cid_found}. Skipping processing.")
            batch_info.append({'start': batch_start_date.date(), 'end': batch_end_date.date(), 'cid': cid_found})
            continue

        cli_script_path = Path(__file__).parent / "cli.py"

        command = [
            sys.executable,  # Use the same python interpreter
            str(cli_script_path),        # The current script file
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
            p_to_wait_on = processes.pop(0)
            stdout, _ = p_to_wait_on.communicate()
            if p_to_wait_on.returncode != 0:
                eprint(f"ERROR: Subprocess failed with return code {p_to_wait_on.returncode}. Aborting.")
                raise RuntimeError("A subprocess failed. Aborting.")
            batch_cid = stdout.strip()
            batch_info[i - len(processes)]['cid'] = batch_cid
            eprint(f"Collected CID: {batch_cid}")

    for i, proc in enumerate(processes):
        stdout, _ = proc.communicate()
        if proc.returncode != 0:
            eprint(f"ERROR: Subprocess failed with return code {proc.returncode}. Aborting.")
            raise RuntimeError("A subprocess failed. Aborting.")
        batch_cid = stdout.strip()
        info_index = len(batch_info) - len(processes) + i
        batch_info[info_index]['cid'] = batch_cid
        eprint(f"Collected CID: {batch_cid}")

    batch_cids = [info['cid'] for info in batch_info if info['cid']]

    # Graft batches into the skeleton store
    skeleton_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=False, root_cid=CID.decode(extended_cid))
    start_time = time.perf_counter()
    running_chunk_offset -= HOURS_PER_BATCH // chunking_settings['time']
    for batch_cid in batch_cids:
        running_chunk_offset += HOURS_PER_BATCH // chunking_settings['time']
        current_graft_location = (running_chunk_offset, 0, 0)
        await skeleton_store.graft_store(batch_cid, current_graft_location)
    end_time = time.perf_counter()
    eprint(f"Time taken to graft all batches: {end_time - start_time:.2f} seconds")

    # Flush the final store
    final_cid = await skeleton_store.flush()
    save_cid_to_file(final_cid, dataset, start_date, end_date, "full")
    eprint(f"Full dataset created with CID: {final_cid}")
    # One Final Check
    await run_checks(cid=final_cid, dataset_name=dataset, num_checks=1000, start_date=start_date, end_date=end_date)
    print(final_cid)
