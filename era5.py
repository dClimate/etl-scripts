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

scratchspace: Path = (Path(__file__).parent / "scratchspace" / "era5").absolute()
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


def is_file_on_r2(key: str) -> bool:
    try:
        r2.head_object(Bucket=era5_env["BUCKET_NAME"], Key=key)
        return True
    except S3ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            # Something else has gone wrong
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


def next_month(dt: datetime) -> datetime:
    """Return the next month"""
    y = dt.year
    m = dt.month
    if m == 12:
        y += 1
        m = 1
    else:
        m += 1
    print(f"Next month for {dt} is {y}-{m}")
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


def download_grib(
    dataset: str,
    timestamp: datetime,
    period: str,
    force=False,
    api_key: str | None = None,
) -> Path:
    """
    See the download command for more on the logic of this function. That click command just exists as a wrapper to expose this to the CLI.

    only_hour specifies if to only download an hour's worth of data, rather than just the whole day that hour belongs to.
    """
    if dataset not in dataset_names:
        raise ValueError(f"Invalid dataset value {dataset}")
    if period not in period_options:
        raise ValueError(f"Invalid period {period}")

    download_filepath = make_grib_filepath(dataset, timestamp, period)
    file_on_disk = download_filepath.exists()

    filename = download_filepath.name
    file_on_r2 = is_file_on_r2(filename)

    upload_to_r2_at_end = False

    # If we're either being forced to redownload, or the file is not on both R2 and disk, then redownload and upload to R2 when done
    if force or (not file_on_r2 and not file_on_disk):
        upload_to_r2_at_end = True
    else:
        if file_on_r2 and file_on_disk:
            eprint(f"File {filename} on both R2 and disk, skipping download")
            pass
        elif file_on_r2 and not file_on_disk:
            eprint(f"Downloading from R2 to local filepath {download_filepath}")
            r2.download_file(era5_env["BUCKET_NAME"], filename, download_filepath)
        elif not file_on_r2 and file_on_disk:
            eprint(f"Uploading from local file {download_filepath} to R2")
            r2.upload_file(download_filepath, era5_env["BUCKET_NAME"], filename)
        return download_filepath

    # List of times in the format HH:MM
    hour_request: list[str] = []
    day_request: list[str] = []
    all_hours = hour_request = [
        "00:00",
        "01:00",
        "02:00",
        "03:00",
        "04:00",
        "05:00",
        "06:00",
        "07:00",
        "08:00",
        "09:00",
        "10:00",
        "11:00",
        "12:00",
        "13:00",
        "14:00",
        "15:00",
        "16:00",
        "17:00",
        "18:00",
        "19:00",
        "20:00",
        "21:00",
        "22:00",
        "23:00",
    ]
    match period:
        case "hour":
            hour_request = [timestamp.strftime("%H:00")]
            day_request = [timestamp.strftime("%d")]
        case "day":
            hour_request = all_hours
            day_request = [timestamp.strftime("%d")]
        case "month":
            hour_request = all_hours
            # doing a .replace returns a new datetime entirely, so we can just assign to our end date like this
            end = next_month(timestamp)
            delta = (end - timestamp).days
            for i in range(0, delta):
                day_request.append((timestamp + timedelta(days=i)).strftime("%d"))

    request = {
        "product_type": ["reanalysis"],
        "variable": [dataset],
        "year": [timestamp.strftime("%Y")],  # YYYY
        "month": [timestamp.strftime("%m")],  # MM
        "day": day_request,  # list in DD format
        "time": hour_request,
        "data_format": "grib",
        "download_format": "unarchived",
    }

    client: cdsapi.Client
    if api_key is None:
        client = cdsapi.Client()
    else:
        client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=api_key)
    print(f"=== Downloading to {download_filepath}")
    # Note that this will overwrite an existing GRIB file, that's the CDS API default behavior when specifying the same filepath
    client.retrieve("reanalysis-era5-single-levels", request, download_filepath)

    if upload_to_r2_at_end:
        eprint(f"Uploading from local file {download_filepath} to R2")
        r2.upload_file(download_filepath, era5_env["BUCKET_NAME"], filename)

    return download_filepath


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

@click.command
@click.argument("dataset", type=datasets_choice)
def get_available_timespan(dataset: str):
    latest = get_latest_timestamp(dataset)
    earliest = "1940-01-01T00:00:00"
    return f"{earliest} {latest}"


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("timestamp", type=click.DateTime())
@click.argument("period", type=period_choice)
@click.option(
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Force a redownload even if the data exists on disk and/or R2. This will also overwrite any preexisting files on both disk and R2.",
)
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
def download(
    dataset: str, timestamp: datetime, period: str, force: bool, api_key: str | None
):
    """
    Downloads the GRIB file for the timestamp. The period specifies whether to download data for the hour, day, or month that timestamp belongs in.

    If the file is on R2 and not on disk, this downloads to disk.
    If the file is on disk and not on R2, then this will upload a copy to R2.
    If the file is in neither locations, this will download to disk and then upload to R2.
    """
    download_grib(dataset, timestamp, period, force=force, api_key=api_key)


@click.command
@click.argument(
    "path",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
def check_finalized(path: Path):
    """Checks if an ERA5 GRIB data file has finalized data. Assumes there is only one data variable inside the GRIB file, which is the case usually for ERA5. Writes true if finalized, false if preliminary."""
    # assume path exist due to click argument verification

    ds = xr.open_dataset(path, backend_kwargs={"read_keys": ["expver"]})

    is_finalized: bool = False
    for v in ds.data_vars:
        is_finalized = int(ds[v].GRIB_expver) == 1

    if is_finalized:
        print("true")
    else:
        print("false")


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

    if len(ds.valid_time.dims) == 2:
        # Monthly downloaded files have their time coordinate as an N-dimensional array, make this coordinate linear
        ds_stack = ds.stack(throwaway=("time", "step"))
        ds_linear = ds_stack.swap_dims({"throwaway": "valid_time"})
        ds = ds_linear.drop_vars(["throwaway"])

    ds = ds.drop_vars(["number", "step", "surface", "time"])
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

@click.command
@click.argument("dataset", type=datasets_choice)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
def instantiate(
    dataset: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
):

    """
    Creates a new, chunk-aligned Zarr store on IPFS, initialized with
    the first 1200 hours (50 days) of data.
    """

    # We want the initial store to contain 1200 hours of data, which is
    # exactly 3 Zarr time chunks (3 * 400h) and 50 days (50 * 24h).
    INITIAL_HOURS_TO_DOWNLOAD = 1200

    NUM_DAYS_NEEDED = INITIAL_HOURS_TO_DOWNLOAD // 24

    start_date = datetime(1940, 1, 1)

    eprint(f"Downloading initial {NUM_DAYS_NEEDED} days of data to create aligned store...")

    grib_paths: list[Path] = []
    current_day = start_date - timedelta(days=1) # Start before the first day to make loop simpler
    for _ in range(NUM_DAYS_NEEDED):
        current_day += timedelta(days=1)
        path = download_grib(dataset, current_day, "day", api_key=api_key)
        grib_paths.append(path)

    # Download the months that contain the start to end date
    # current = start_date
    # while current < end_date:
    #     eprint(f"Downloading GRIB for month of date {current}")
    #     path = download_grib(dataset, current, "month", api_key=api_key)
    #     grib_paths.append(path)
    #     current = next_month(current)


    eprint("====== Writing this dataset to a new Zarr on IPFS ======")
    ds = xr.open_mfdataset(grib_paths)
    ds = standardize(dataset, ds)
    eprint(ds)

    eprint(f"Forcing encoding with chunk settings: {chunking_settings}")
    
    # Set encoding for the data variable
    encoding_chunks = tuple(chunking_settings.get(dim) for dim in ds[dataset].dims)
    ds[dataset].encoding['chunks'] = encoding_chunks

    for coord_name, coord_array in ds.coords.items():
        if coord_name in chunking_settings:
            ds[coord_name].encoding['chunks'] = (chunking_settings[coord_name],)

    ordered_dims = list(ds.dims)
    array_shape = tuple(ds.dims[dim] for dim in ordered_dims)
    chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)

    # Reorder to be time, latitude, longitude
    if ordered_dims != ["time", "latitude", "longitude"]:
        ordered_dims = ["time", "latitude", "longitude"]
        ds = ds.transpose(*ordered_dims)
        array_shape = tuple(ds.dims[dim] for dim in ordered_dims)
        chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)
    eprint("Ordered dimensions, array shape, chunk shape:")

    print(ordered_dims, array_shape, chunk_shape)


    async def _upload_to_ipfs():
        async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
            store_write = await ShardedZarrStore.open(
                cas=kubo_cas,
                array_shape=array_shape,
                chunk_shape=chunk_shape,
                chunks_per_shard=26000,
                read_only=False,
            )
            start_time = time.perf_counter()
            ds.to_zarr(store=store_write, mode="w")
            end_time = time.perf_counter()
            eprint(f"Time taken to write dataset: {end_time - start_time:.2f} seconds")
            root_cid = await store_write.flush()
            eprint("New ShardedZarrStore CID:")
            print(root_cid)

    asyncio.run(_upload_to_ipfs())

    eprint("Cleaning up GRIB files...")
    # for path in grib_paths:
    #     os.remove(path)

async def chunked_write(ds: xr.Dataset, variable_name: str, kubo_cas: KuboCAS) -> str:
    # Note: I've modified it slightly to accept an existing KuboCAS instance
    #       and return the CID as a string for easier use.
    ordered_dims = list(ds.dims)
    array_shape = tuple(ds.dims[dim] for dim in ordered_dims)
    chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)
    if ordered_dims[0] != 'time':
        ds = ds.transpose('time', 'latitude', 'longitude', ...)
        ordered_dims = list(ds.dims)
        array_shape = tuple(ds.dims[dim] for dim in ordered_dims)
        chunk_shape = tuple(ds.chunks[dim][0] for dim in ordered_dims)

    store_write = await ShardedZarrStore.open(
        cas=kubo_cas,
        array_shape=array_shape,
        chunk_shape=chunk_shape,
        chunks_per_shard=26000,
        read_only=False,
    )
    print("Writing dataset to Zarr store...")
    ds.to_zarr(store=store_write, mode="w")
    root_cid = await store_write.flush()
    return str(root_cid)


# @click.command()
# @click.argument("dataset", type=datasets_choice)
# @click.argument("cid", type=str)
# @click.option(
#     "--end-date",
#     type=click.DateTime(),
#     required=True,
#     help="The target end date to extend the dataset to.",
# )
# @click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
# @click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
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
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
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
            np.datetime64(end_date) + np.timedelta64(1, 'h'),
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

        time_chunk_size = chunking_settings['time']
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
        zarr.consolidate_metadata(main_store)
        
        # --- 4. Flush and output the new CID ---
        extended_cid = await main_store.flush()
        eprint("\n--- Extend Operation Complete! ---")
        eprint("You can now use this new CID to graft data into the empty space.")
        eprint(f"Extended Dataset CID: {extended_cid}")
        return extended_cid


    

@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.option(
    "--end-date",
    type=click.DateTime(),
    required=False,
    help="The target date to append data up to (inclusive). Format: YYYY-MM-DD",
)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
def append(
    dataset,
    cid: str,
    end_date: datetime | None,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    api_key: str | None,
):
    """
    Append the data at timestamp onto the Dataset that cid points to, print out the CID of the new HAMT root.

    This command requires the kubo daemon to be running.

    Appends in steps of months is recommended due to Copernicus recommending month-by-month downloading from their API.
    https://confluence.ecmwf.int/display/CKB/Climate+Data+Store+%28CDS%29+documentation#ClimateDataStore(CDS)documentation-Efficiencytips
    https://forum.ecmwf.int/t/ecmwf-apis-faq-api-data-documentation/6880
    """
    if end_date is None:
        end_date = get_latest_timestamp(dataset, api_key=api_key)
    async def _do_append():
        # The batch size must be the Least Common Multiple of the data unit (24h)
        # and the Zarr chunk size (400h). LCM(24, 400) = 1200.
        HOURS_PER_BATCH = 1200
        DAYS_PER_BATCH = HOURS_PER_BATCH // 24
        main_cid = cid

        async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
 
            # 1. Open the initial store
            initial_store_ro = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=cid)
            if initial_store_ro._root_obj is None:
                raise ValueError("Could not load the initial store's root object. Is the CID valid?")
            initial_ds = xr.open_zarr(initial_store_ro)
            # 2. Find the end timestamp in the existing data for the starting point of the append
            start_timestamp = npdt_to_pydt(initial_ds.time[-1].values)
            eprint(f"Last timestamp in existing data: {start_timestamp.date()}")

            initial_shape = initial_store_ro._array_shape
            initial_chunks_per_dim = initial_store_ro._chunks_per_dim

            # Crucially, get the original time values as a numpy array
            initial_time_values = initial_ds.time.values
            time_dim_index = initial_ds.dims['time']
            time_chunk_size = initial_ds.chunks['time'][0]

            # THIS OFFSET IS CRUCIAL AS IT DETERMINES WHERE THE NEW DATA WILL BE GRAFTED BEFORE THE SKELETON IS CREATED
            running_chunk_offset = initial_chunks_per_dim[0]
            print(f"Running chunk offset: {running_chunk_offset}")

            initial_ds.close()

            eprint(f"Starting append from {start_timestamp.date()}. Target end date: {end_date.date()}")

            all_days_to_download = []
            current_ts = start_timestamp
            while current_ts < end_date:
                current_ts += timedelta(days=1)
                all_days_to_download.append(current_ts)

            if not all_days_to_download:
                eprint("No new days to download.")
                return

            # We can only download in batches of 50 days, so we will split the days into batches
            batches_of_days = [
                all_days_to_download[i:i + DAYS_PER_BATCH]
                for i in range(0, len(all_days_to_download), DAYS_PER_BATCH)
            ]
            eprint(f"Planned {len(batches_of_days)} batches of up to {DAYS_PER_BATCH} days each.")

            # Create the skeleton for the new time dimension
            extended_cid = await extend(dataset, cid, end_date, gateway_uri_stem, rpc_uri_stem)
            if not extended_cid:
                eprint("Failed to extend dataset.")
                return

            async def test_append(days: list[datetime]) -> str:
                """Helper that creates one 1200-hour store and returns its CID."""
                if not days: return None

                
            # For testing, do only the first 2 batches
            start_time = time.perf_counter()
            batches_of_days = batches_of_days[:2]
            batch_cids = []
            for i, batch in enumerate(batches_of_days):
                grib_paths = []
                for ts in batch:
                    grib_path = download_grib(dataset, ts, "day", api_key=api_key)
                    grib_paths.append(grib_path)
                ds = xr.open_mfdataset(grib_paths, engine='cfgrib')
                ds = standardize(dataset, ds)
                batch_cid = await chunked_write(ds, dataset, kubo_cas)
                batch_cids.append(batch_cid)

            end_time = time.perf_counter()
            eprint(f"Time taken to create all batches: {end_time - start_time:.2f} seconds")
            eprint(f"Batch CIDs: {batch_cids}")

            # Skeleton Store to graft into
            skeleton_store = await ShardedZarrStore.open(cas=kubo_cas, read_only=False, root_cid=CID.decode(extended_cid))

            for i, batch_cid in enumerate(batch_cids):
                # Calculate the offset for this graft
                running_chunk_offset += i * HOURS_PER_BATCH // 400
                current_graft_location = (running_chunk_offset, 0, 0)
                
                # Perform the graft
                await skeleton_store.graft_store(batch_cid, current_graft_location)
            
            # 4. Flush the main store ONCE at the end
            final_cid = await skeleton_store.flush()
            eprint(f"\nAll batches grafted successfully! Final Root CID: {final_cid}")

            # print(new_cid)
            eprint("Removing GRIBs on disk")
            # for path in grib_paths:
            #     os.remove(path)
    asyncio.run(_do_append())


@click.group
def cli():
    """
    Commands for ETLing ERA5. On invocation, a scratch space directory relative to this file will be created for data files at ./scratchspace/era5.
    """
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)
cli.add_command(check_finalized)
cli.add_command(instantiate)
cli.add_command(append)
# cli.add_command(backfill)
# cli.add_command(extend)

if __name__ == "__main__":
    cli()
