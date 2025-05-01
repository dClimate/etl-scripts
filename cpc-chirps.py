import os
import subprocess
from datetime import UTC, datetime, timedelta
from math import ceil
from pathlib import Path

import click
import numpy as np
import xarray as xr
import numcodecs
from multiformats import CID
from py_hamt import HAMT

from abc import ABC, abstractmethod
from dag_cbor.ipld import IPLDKind
import requests
from msgspec import json
from multiformats import multihash
from multiformats import CID
from multiformats.multihash import Multihash

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

class Store(ABC):
    """Abstract class that represents a storage mechanism the HAMT can use for keeping data.

    The return type of save and input to load is really type IPLDKind, but the documentation generates something a bit strange since IPLDKind is a type union.
    """

    @abstractmethod
    def save_raw(self, data: bytes) -> IPLDKind:
        """Take any set of bytes, save it to the storage mechanism, and return an ID in the type of IPLDKind that can be used to retrieve those bytes later."""

    @abstractmethod
    def save_dag_cbor(self, data: bytes) -> IPLDKind:
        """Take a set of bytes and save it just like `save_raw`, except this method has additional context that the data is in a dag-cbor format."""

    @abstractmethod
    def load(self, id: IPLDKind) -> bytes:
        """Retrieve the bytes based on an ID returned earlier by the save function."""


# Inspired by https://github.com/rvagg/iamap/blob/master/examples/memory-backed.js
class DictStore(Store):
    """A basic implementation of a backing store, mostly for demonstration and testing purposes. It hashes all inputs and uses that as a key to an in-memory python dict. The hash bytes are the ID that `save` returns and `load` takes in."""

    store: dict[bytes, bytes]
    """@private"""
    hash_alg: Multihash
    """@private"""

    def __init__(self):
        self.store = {}
        self.hash_alg = multihash.get("blake3")

    def save(self, data: bytes) -> bytes:
        hash = self.hash_alg.digest(data, size=32)
        self.store[hash] = data
        return hash

    def save_raw(self, data: bytes) -> bytes:
        """"""
        return self.save(data)

    def save_dag_cbor(self, data: bytes) -> bytes:
        """"""
        return self.save(data)

    # Ignore the type error since bytes is in the IPLDKind type
    def load(self, id: bytes) -> bytes:  # type: ignore
        """"""
        if id in self.store:
            return self.store[id]
        else:
            raise Exception("ID not found in store")


class IPFSStore(Store):
    """
    Use IPFS as a backing store for a HAMT. The IDs returned from save and used by load are IPFS CIDs.

    Save methods use the RPC API but `load` uses the HTTP Gateway, so read-only HAMTs will only access the HTTP Gateway. This allows for connection to remote gateways as well.

    You can write to an authenticated IPFS node by providing credentials in the constructor. The following authentication methods are supported:
    - Basic Authentication: Provide a tuple of (username, password) to the `basic_auth` parameter.
    - Bearer Token: Provide a bearer token to the `bearer_token` parameter.
    - API Key: Provide an API key to the `api_key` parameter. You can customize the header name for the API key by setting the `api_key_header` parameter.
    """

    def __init__(
        self,
        timeout_seconds: int = 30,
        gateway_uri_stem: str = "http://127.0.0.1:8080",
        rpc_uri_stem: str = "http://127.0.0.1:5001",
        hasher: str = "blake3",
        pin_on_add: bool = False,
        debug: bool = False,
        # Authentication parameters
        basic_auth: tuple[str, str] | None = None,  # (username, password)
        bearer_token: str | None = None,
        api_key: str | None = None,
        api_key_header: str = "X-API-Key",  # Customizable API key header
    ):
        self.timeout_seconds = timeout_seconds
        """
        You can modify this variable directly if you choose.

        This sets the timeout in seconds for all HTTP requests.
        """
        self.gateway_uri_stem = gateway_uri_stem
        """
        URI stem of the IPFS HTTP gateway that IPFSStore will retrieve blocks from.
        """
        self.rpc_uri_stem = rpc_uri_stem
        """URI Stem of the IPFS RPC API that IPFSStore will send data to."""
        self.hasher = hasher
        """The hash function to send to IPFS when storing bytes."""
        self.pin_on_add: bool = pin_on_add
        """Whether IPFSStore should tell the daemon to pin the generated CIDs in API calls. This can be changed in between usage, but should be kept the same value for the lifetime of the program."""
        self.debug: bool = debug
        """If true, this records the total number of bytes sent in and out of IPFSStore to the network. You can access this information in `total_sent` and `total_received`. Bytes are counted in terms of either how much was sent to IPFS to store a CID, or how much data was inside of a retrieved IPFS block. This does not include the overhead of the HTTP requests themselves."""
        self.total_sent: None | int = 0 if debug else None
        """Total bytes sent to IPFS. Used for debugging purposes."""
        self.total_received: None | int = 0 if debug else None
        """Total bytes in responses from IPFS for blocks. Used for debugging purposes."""

        # Authentication settings
        self.basic_auth = basic_auth
        """Tuple of (username, password) for Basic Authentication"""
        self.bearer_token = bearer_token
        """Bearer token for token-based authentication"""
        self.api_key = api_key
        """API key for API key-based authentication"""
        self.api_key_header = api_key_header
        """Header name to use for API key authentication"""

    def save(self, data: bytes, cid_codec: str) -> CID:
        """
        This saves the data to an ipfs daemon by calling the RPC API, and then returns the CID, with a multicodec set by the input cid_codec. We need to do this since the API always returns either a multicodec of raw or dag-pb if it had to shard the input data.

        By default, `save` pins content it adds.

        ```python
        from py_hamt import IPFSStore

        ipfs_store = IPFSStore()
        cid = ipfs_store.save("foo".encode(), "raw")
        print(cid.human_readable)
        ```
        """
        pin_string: str = "true" if self.pin_on_add else "false"

        # Apply authentication based on provided credentials
        headers = {}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        elif self.api_key:
            headers[self.api_key_header] = self.api_key

        # Prepare request parameters
        url = f"{self.rpc_uri_stem}/api/v0/add?hash={self.hasher}&pin={pin_string}"

        # Make the request with appropriate authentication
        response = requests.post(
            url,
            files={"file": data},
            headers=headers,
            auth=self.basic_auth,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        cid_str: str = json.decode(response.content)["Hash"]  # type: ignore
        cid = CID.decode(cid_str)
        # If it's dag-pb it means we should not reset the cid codec, since this is a UnixFS entry for a large amount of data that thus had to be sharded
        # We don't worry about HAMT nodes being larger than 1 MB
        # with a conservative calculation of 256 map keys * 10 (bucket size of 9 and 1 link per map key)*100 bytes huge size for a cid=0.256 MB, so we can always safely recodec those as dag-cbor, which is what they are
        # 0x70 means dag-pb
        if cid.codec.code != 0x70:
            cid = cid.set(codec=cid_codec)

        # if everything is succesful, record debug information
        if self.debug:
            self.total_sent += len(data)  # type: ignore

        return cid

    def save_raw(self, data: bytes) -> CID:
        """See `save`"""
        return self.save(data, "raw")

    def save_dag_cbor(self, data: bytes) -> CID:
        """See `save`"""
        return self.save(data, "dag-cbor")

    # Ignore the type error since CID is in the IPLDKind type
    def load(self, id: CID) -> bytes:  # type: ignore
        """
        This retrieves the raw bytes by calling the provided HTTP gateway.
        ```python
        from py_hamt import IPFSStore
        from multiformats import CID

        ipfs_store = IPFSStore()
        # This is just an example CID
        cid = CID.decode("bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze")
        data = ipfs_store.load(cid)
        print(data)
        ```
        """
        response = requests.get(
            f"{self.gateway_uri_stem}/ipfs/{str(id)}", timeout=self.timeout_seconds
        )
        response.raise_for_status()

        if self.debug:
            self.total_received += len(response.content)  # type: ignore

        return response.content


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
        da.encoding["compressors"] = numcodecs.Blosc(clevel=9)

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

    ipfs_store = IPFSStore(api_key="fb1b43fb-5ea9-4738-8deb-04dc725cae72", gateway_uri_stem="https://ipfs-gateway.dclimate.net/ipfs", 
    rpc_uri_stem="https://ipfs-gateway.dclimate.net")
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem
    ipfszarr3 = HAMT(store=ipfs_store)
    ds.to_zarr(store=ipfszarr3)  # type: ignore
    eprint("HAMT CID")
    print(ipfszarr3.root_node_id)


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
    ipfs_store = IPFSStore(api_key="fb1b43fb-5ea9-4738-8deb-04dc725cae72", gateway_uri_stem="https://ipfs-gateway.dclimate.net/ipfs", 
    rpc_uri_stem="https://ipfs-gateway.dclimate.net")
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem
    hamt = HAMT(store=ipfs_store, root_node_id=CID.decode(cid))
    ipfszarr3 = hamt
    ipfs_ds = xr.open_zarr(store=ipfszarr3)
    ipfs_latest_timestamp = npdt_to_pydt(ipfs_ds.time[-1].values)

    timestamp: datetime = ipfs_latest_timestamp
    for c in range(0, count):
        if year:
            timestamp = timestamp.replace(year=timestamp.year + 1)
        else:
            timestamp = timestamp + timedelta(days=1)

        eprint("====== Creating dataset to append ======")
        nc_path: Path
        if not skip_download:
            eprint(f"Downloading netCDF for year {timestamp.year}...")
            nc_path = download_year(dataset, timestamp.year)
        else:
            eprint(
                f"Skipping downloading netCDF for year {timestamp.year}, going to try use what's on disk"
            )
            nc_path = make_nc_path(dataset, timestamp.year)
        ds = xr.open_dataset(nc_path)
        ds = standardize(dataset, ds)

        if year:
            ds = ds.sel(
                time=str(timestamp.year)
            )  # conversion to string aggregates all timestamps within the year
        else:
            ds = ds.sel(
                time=slice(timestamp, timestamp)
            )  # without slice, time becomes a scalar and an append will not succeed

        eprint(ds)

        eprint("====== Appending to IPFS ======")
        ds.to_zarr(store=ipfszarr3, append_dim="time")  # type: ignore
        eprint("HAMT CID")
        print(ipfszarr3.root_node_id)


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
