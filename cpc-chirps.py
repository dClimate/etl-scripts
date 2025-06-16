import os
import subprocess
from datetime import UTC, datetime, timedelta
from math import ceil
from pathlib import Path

import click
import numpy as np
import xarray as xr
import zarr.codecs
from multiformats import CID
from py_hamt import HAMT, ShardedZarrStore, KuboCAS
import asyncio

from etl_scripts.grabbag import eprint, npdt_to_pydt

scratchspace: Path = (Path(__file__).parent / "scratchspace" / "cpc-chirps").absolute()
os.makedirs(scratchspace, exist_ok=True)

dataset_names = [
    "cpc-precip-conus",
    "cpc-precip-global",
    "cpc-tmin",
    "cpc-tmax",
    "chirps-final-p05",
    "chirps-final-p25",
    "chirps-prelim-p05",
]
datasets_choice = click.Choice(dataset_names)


def make_nc_path(dataset: str, year: int) -> Path:
    return scratchspace / f"{dataset}_{year}.nc"


def download_year(dataset: str, year: int) -> Path:
    """
    Downloads the nc file for that year, and returns a path to it. Idempotent since it uses curl's timestamping check availability.

    Raises ValueError on invalid dataset. Raises Exception if download fails.
    """
    year_url: str
    match dataset:
        case "cpc-precip-conus":
            year_url = f"https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_us_precip/RT/precip.V1.0.{year}.nc"
        case "cpc-precip-global":
            year_url = f"https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_precip/precip.{year}.nc"
        case "cpc-tmax":
            year_url = f"https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmax.{year}.nc"
        case "cpc-tmin":
            year_url = f"https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmin.{year}.nc"
        case "chirps-final-p05":
            year_url = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc"
        case "chirps-final-p25":
            year_url = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25/chirps-v2.0.{year}.days_p25.nc"
        case "chirps-prelim-p05":
            year_url = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc"
        case _:
            raise ValueError(f"Invalid dataset {dataset}")

    nc_path = make_nc_path(dataset, year)

    curl_result = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",  # if we get a 404 then show it
            "--fail",  # return with status 22 on bad HTTP response code
            "-o",  # Specify output filepath
            nc_path,
            "-z",  # Only download if either the file does not exist or the remote file is newer
            nc_path,
            year_url,
        ]
    )
    if curl_result.returncode != 0:
        raise Exception("curl returned nonzero exit code")
    return nc_path


# See README.md for chunking decision
chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


def standardize(dataset: str, ds: xr.Dataset) -> xr.Dataset:
    if dataset.startswith("cpc"):
        ds = ds.rename({"lat": "latitude", "lon": "longitude"})
        # Remove unneeded metadata
        del ds.time.attrs["actual_range"]
        del ds.time.attrs["delta_t"]
        del ds.time.attrs["avg_period"]

    ds = ds.sortby("latitude", ascending=True)
    ds = ds.sortby("longitude", ascending=True)

    if dataset.startswith("cpc"):
        # CPC's longitude stretches from 0 to 360, this changes to -180 (west) to 180 (east)
        new_longitude = np.where(ds.longitude > 180, ds.longitude - 360, ds.longitude)
        ds = ds.assign_coords(longitude=new_longitude)
        ds = ds.sortby("longitude", ascending=True)

    ds = ds.chunk(chunking_settings)

    for var in ds.data_vars:
        da = ds[var]

        # Apply compression
        da.encoding["compressors"] = zarr.codecs.BloscCodec()

        # Prefer _FillValue over missing_value
        da.encoding["_FillValue"] = np.nan
        if "missing_value" in da.attrs:
            del da.attrs["missing_value"]
        if "missing_value" in da.encoding:
            del da.encoding["missing_value"]

    return ds


dataset_start_years = {
    "cpc-precip-conus": 2007,
    "cpc-precip-global": 1979,
    "cpc-tmax": 1979,
    "cpc-tmin": 1979,
    "chirps-final-p05": 1981,
    "chirps-final-p25": 1981,
    "chirps-prelim-p05": 2015,
}


@click.command
@click.argument("dataset", type=datasets_choice)
def get_available_timespan(dataset):
    """
    Gets the earliest and latest timestamps for this dataset and prints to stdout. Output looks like "earliest latest".
    """
    start_year = dataset_start_years[dataset]
    eprint(
        f"Downloading netCDF of start year {start_year} to see earliest data coverage"
    )
    earliest_nc = download_year(dataset, start_year)

    current_year = datetime.now(UTC).year
    eprint(
        f"Downloading netCDF of current year {current_year} to see latest data coverage"
    )
    latest_nc = download_year(dataset, current_year)

    ds_earliest = xr.open_dataset(earliest_nc)

    ds_latest = xr.open_dataset(latest_nc)
    # Can happen right after the start of a new year, you'll get netCDf files with no data
    if len(ds_latest.time) == 0:
        eprint(
            f"Found no data in nc file of year {current_year}, downloading prior year"
        )
        latest_nc = download_year(dataset, current_year - 1)
        ds_latest = xr.open_dataset(latest_nc)

    earliest_dt64: np.datetime64 = ds_earliest.time[0].values
    latest_dt64: np.datetime64 = ds_latest.time[len(ds_latest.time) - 1].values

    earliest_dt: datetime = datetime.fromisoformat(earliest_dt64.astype(str))
    latest_dt: datetime = datetime.fromisoformat(latest_dt64.astype(str))

    # Output YYYY-MM-DD ISO8601 format to reflect that CPC/CHIRPS data precision is in days
    earliest = earliest_dt.strftime("%Y-%m-%d")
    latest = latest_dt.strftime("%Y-%m-%d")
    print(f"{earliest} {latest}")


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("timestamp", type=click.DateTime())
def download(dataset, timestamp: datetime):
    """Download to the scratchspace the netCDF file that contains the data for the timestamp, which should be formatted in ISO8601.

    e.g. uv run cpc.py download precip-conus 2014-01-01
    """
    year = timestamp.year
    eprint(f"Downloading netCDF for year {year}")
    download_year(dataset, year)


@click.command
@click.argument("dataset", type=datasets_choice)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--skip-download",
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip downloading data. Useful when remote data provider servers are inaccessible.",
)
def instantiate(
    dataset: str,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    skip_download: bool,
):
    """
    Create an entirely new zarr on ipfs for this dataset, return new HAMT CID on stdout.

    This command requires the kubo daemon to be running.

    This solves issues with chunking, appending onto an empty dataset with singular or yearly timestamps results in improperly chunked data variables, the chunking for the time dimension will be set to the lowest of however much data there is. For example, if appending onto an empty dataset with year's worth of data, when going to append again you will find that the chunking has been set to 365 permanently, which would otherwise require a rechunk and rewrite.
    """
    time_chunk = chunking_settings["time"]
    num_years_needed = ceil(time_chunk / 365.25)
    start_year = dataset_start_years[dataset]
    end_year = start_year + num_years_needed - 1
    eprint(
        f"Downloading netCDFs for years {start_year} to {end_year} so that time dimension is properly filled for chunk parameter {time_chunk}"
    )
    nc_paths: list[Path] = []
    for year in range(start_year, end_year + 1):
        path: Path
        if not skip_download:
            eprint(f"Downloading year {year}")
            path = download_year(dataset, year)
        else:
            eprint(
                f"Told to skip download, otherwise would have downloaded year {year} here"
            )
            path = make_nc_path(dataset, year)
        nc_paths.append(path)

    eprint("====== Writing this dataset to a new Zarr on IPFS ======")
    ds = xr.open_mfdataset(nc_paths)
    ds = standardize(dataset, ds)
    eprint(ds)

    ordered_dims = list(ds.dims)
    array_shape_tuple = tuple(ds.dims[dim] for dim in ordered_dims)
    chunk_shape_tuple = tuple(ds.chunks[dim][0] for dim in ordered_dims)

    async def _upload_to_ipfs():
        """Define and run an async function to handle the async IPFS operations."""
        async with KuboCAS(
            rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem
        ) as kubo_cas:
            store_write = await ShardedZarrStore.open(
                cas=kubo_cas,
                read_only=False,
                array_shape=array_shape_tuple,
                chunk_shape=chunk_shape_tuple,
                chunks_per_shard=26000,
            )
            ds.to_zarr(store=store_write, mode="w")
            root_cid = await store_write.flush()
            eprint("ShardedZarrStore CID")
            print(root_cid)

    asyncio.run(_upload_to_ipfs())


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--year",
    is_flag=True,
    show_default=True,
    default=False,
    help="Append the entire year of the timestamp. This will ignore the month and day of the timestamp.",
)
@click.option(
    "--skip-download",
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip downloading data. Useful when remote data provider servers are inaccessible.",
)
@click.option(
    "--count",
    default=1,
    show_default=True,
    help="The number of days/years to append. Usually 1, but if increased append will repeatedly print to stdout the CID of each successive append. This will essentially repeat the normal 1 count append command.",
)
def append(
    dataset,
    cid: str,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    year: bool,
    skip_download: bool,
    count: int,
):
    """
    Append the data at timestamp onto the Dataset that CID points to, print new HAMT root CID to stdout.

    This command requires the kubo daemon to be running.
    """
    async def _do_append():
        async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
            # First, open the store in read-only mode to get the latest timestamp
            eprint("====== Reading latest timestamp from existing Zarr ======")
            store_read = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=CID.decode(cid))
            ipfs_ds = xr.open_zarr(store_read)
            ipfs_latest_timestamp = npdt_to_pydt(ipfs_ds.time[-1].values)
            eprint(f"Latest timestamp found: {ipfs_latest_timestamp.isoformat()}")

            current_cid = CID.decode(cid)
            timestamp: datetime = ipfs_latest_timestamp

            # Loop for each append operation
            for i in range(count):
                if year:
                    timestamp = timestamp.replace(year=timestamp.year + 1)
                else:
                    timestamp = timestamp + timedelta(days=1)

                eprint(f"\n====== Preparing to append data for {timestamp.date()} (Append {i+1}/{count}) ======")
                
                # Download and standardize the new data slice
                nc_path: Path
                if not skip_download:
                    eprint(f"Downloading netCDF for year {timestamp.year}...")
                    nc_path = download_year(dataset, timestamp.year)
                else:
                    eprint(f"Skipping download for year {timestamp.year}, using local file.")
                    nc_path = make_nc_path(dataset, timestamp.year)
                
                ds = xr.open_dataset(nc_path)
                ds = standardize(dataset, ds)

                if year:
                    ds_to_append = ds.sel(time=str(timestamp.year))
                else:
                    # Slicing ensures the time dimension is preserved
                    ds_to_append = ds.sel(time=slice(timestamp, timestamp))
                
                eprint("Dataset to append:")
                eprint(ds_to_append)

                if ds_to_append.time.size == 0:
                    eprint(f"Warning: No data found for {timestamp.date()}. Skipping append.")
                    continue

                # Open the store for writing using the current CID
                eprint("====== Appending to IPFS ======")
                store_write = await ShardedZarrStore.open(cas=kubo_cas, read_only=False, root_cid=current_cid)
                ds_to_append.to_zarr(store=store_write, append_dim="time", mode="a")
                
                # Flush to get the new root CID
                new_cid = await store_write.flush()
                eprint("New ShardedZarrStore CID:")
                print(new_cid)

                # Update the CID for the next iteration
                current_cid = new_cid

    # Run the async append logic
    asyncio.run(_do_append())


@click.group
def cli():
    """
    Commands for ETLing CPC and CHRIPS datasets. On invocation, a scratch space directory relative to this file will be created for data files at ./scratchspace/cpc-chirps.
    """
    pass


cli.add_command(get_available_timespan)
cli.add_command(download)
cli.add_command(instantiate)
cli.add_command(append)

if __name__ == "__main__":
    cli()
