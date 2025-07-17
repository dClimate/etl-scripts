from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Literal

import cfgrib
import numpy as np
import xarray as xr
from ecmwf.opendata import Client
from zarr.codecs import BloscCodec, BloscShuffle

from etl_scripts.grabbag import eprint

# ------------------------------------------------------------------ constants
scratchspace: Path = (Path(__file__).parent.parent / "scratchspace" / "aifs").absolute()
scratchspace.mkdir(parents=True, exist_ok=True)

CHUNKING = {"time": 10, "latitude": 256, "longitude": 256}
COMPRESSOR = BloscCodec(cname="zstd", clevel=4, shuffle=BloscShuffle.bitshuffle)
TIME_AXIS_CHUNK = 100_000  # one slab

ENSEMBLE_SIZE = 50  # AIFS-ENS perturbed members

# ------------------------------------------------------------------ helpers


def aifs_client(source: Literal["ecmwf", "aws", "azure"] = "ecmwf") -> Client:
    """Return a pre-configured opendata Client for the AIFS ensemble."""
    return Client(source=source, model="aifs-ens", resol="0p25")


def download_tprate_slice(
    cli: Client,
    run_time: datetime,
    fhour: int,
    product: Literal["cf", "pf"] = "pf",
) -> Path:
    """
    Download **tprate** (kg m⁻² s⁻¹) for a single lead-time *fhour* (0-360)
    and *product* (`cf` or `pf`) into *scratchspace*.

    The file naming convention keeps things deterministic so your QC &
    deduplication logic remains simple.
    """
    fname = f"{run_time:%Y%m%d%H}00-" f"{fhour:03d}h-enfo-{product}-tprate.grib2"
    target = scratchspace / fname
    if target.exists():
        eprint(f"✓ {target.name} already exists")
        return target

    eprint(f"⇣ Downloading {target.name}")
    cli.retrieve(
        date=run_time.strftime("%Y-%m-%d"),
        time=run_time.hour,
        stream="enfo",
        type=product,  # 'cf' or 'pf'
        step=fhour,
        param="tprate",  #  kg m-2 s-1  == mm s-1
        target=str(target),
    )
    return target


def grib_to_xarray(path: Path) -> xr.DataArray:
    """
    Read a `tprate` GRIB2 slice and convert it from kg m⁻² s⁻¹ to **mm h⁻¹**.
    Works for both PF (50-member) and CF (single-member) files.
    """
    ds = cfgrib.open_dataset(
        str(path),
        backend_kwargs={
            "filter_by_keys": {"shortName": "tprate"},  # keep only precip-rate
            "indexpath": "",  # build tmp index in-mem
        },
    )

    da = ds["tprate"] * 3600.0  # → mm h-1
    da.attrs["units"] = "mm h-1"

    # cfgrib names the ensemble axis "number" – rename for consistency
    if "number" in da.dims:
        da = da.rename({"number": "member"})

    return da


def standardise(arrays: list[xr.DataArray], dataset_name: str) -> xr.Dataset:
    """Apply *common* chunking & compression conventions before writing.

    The helper ensures every dataset written by this script is chunked and
    encoded consistently so that appended data remain compatible and
    downstream tooling (e.g. *fsspec*, *dask*, *kerchunk*) can rely on a
    predictable layout.

    Notes
    -----
    * **Chunking** – Chosen to balance read performance and object size on
      IPFS; 10 dekads (~3 months) per chunk in *time* and gentle tiling in the
      spatial dimensions.
    * **Compression** – Uses Blosc (default codec for many Zarr stores) with
      the default *zstd* parameters. A single codec is attached to every data
      variable; metadata arrays are left untouched.
    * **Missing data** – ``NaN`` is written as the canonical fill value so
      that consumers do not need to guess nodata markers.
    """
    eprint(f"Standardising {dataset_name} dataset…")
    start = time.time()

    # 1) Concatenate the arrays into a single dataset
    ds = xr.concat(arrays, dim="time").to_dataset(name=dataset_name)

    # 2) Dask chunking for computation
    ds = ds.chunk(CHUNKING)

    # 3) Data-variable encodings
    for var in ds.data_vars:
        ds[var].encoding.update(
            {
                "chunks": tuple(CHUNKING[dim] for dim in ds[var].dims),  # type: ignore
                "compressors": [COMPRESSOR],
                "_FillValue": np.nan,
            }
        )

    # 4) Coordinate-variable encodings  (lat/lon/time)
    for coord in ds.coords:
        if coord == "time":
            # one big slab so xarray needs a single GET to read the axis
            ds[coord].encoding["chunks"] = (TIME_AXIS_CHUNK,)
        elif coord in CHUNKING:
            # write the whole axis as one chunk
            ds[coord].encoding["chunks"] = (ds.sizes[coord],)

    eprint(f"✓ Standardised {dataset_name} dataset in {time.time() - start:.2f}s")
    return ds
