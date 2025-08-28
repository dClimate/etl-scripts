from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Literal

import cfgrib
import numpy as np
import requests
import xarray as xr
from ecmwf.opendata import Client
from zarr.codecs import BloscCodec, BloscShuffle

from etl_scripts.grabbag import eprint

# ------------------------------------------------------------------ constants
scratchspace: Path = (Path(__file__).parent.parent / "scratchspace" / "aifs").absolute()
scratchspace.mkdir(parents=True, exist_ok=True)

CHUNKING = {
    "forecast_reference_time": 2,
    "step": 60,
    "latitude": 64,
    "longitude": 64,
}
COMPRESSOR = BloscCodec(cname="zstd", clevel=4, shuffle=BloscShuffle.bitshuffle)
AXIS_CHUNK = 100_000  # one slab
ENSEMBLE_SIZE = 50  # AIFS-ENS perturbed members

# ------------------------------------------------------------------ helpers


class DataNotAvailableError(RuntimeError):
    """Requested open-data file is not available (404)."""


def aifs_client(source: Literal["ecmwf", "aws", "azure"] = "ecmwf") -> Client:
    """Return a pre-configured opendata Client for the AIFS ensemble."""
    return Client(source=source, model="aifs-ens", resol="0p25")


def download_aifs_ens_slice(
    cli: Client,
    date: datetime,
    fhour: int,
    param: str,
    product: Literal["cf", "pf", "em", "es", "ep"] = "cf",
    force: bool = False,
) -> Path:
    """
    Download one GRIB2 "slice" from ECMWF **AIFS ENS** for a single lead time.

    The list of currently available files can be consulted here: https://data.ecmwf.int/forecasts/

    Wraps :class:`ecmwf.opendata.Client` to fetch a parameter (e.g. ``"tp"``) at
    one forecast step from the AIFS Ensemble stream (``stream="enfo"``), for any
    of the supported ensemble products:

    - ``cf`` — Control forecast
    - ``pf`` — Perturbed forecast
    - ``em`` — Ensemble mean
    - ``es`` — Ensemble standard deviation
    - ``ep`` — Ensemble / event probabilities

    Parameters
    ----------
    cli
        Initialized :class:`ecmwf.opendata.Client`.
    date
        Forecast **base time** (cycle) as a timezone-aware UTC ``datetime``.
    fhour
        Forecast lead time **in hours** (e.g., 0–360). Use valid ENS steps.
    param
        GRIB shortName (e.g., ``"tp"``). For ``product="ep"``, use the
        probability product parameter codes published by ECMWF (pre-defined
        events/thresholds per variable).
    product
        One of ``"cf"``, ``"pf"``, ``"em"``, ``"es"``, ``"ep"`` (see above).
    force
        If ``True``, re-download even if file exists.

    Returns
    -------
    pathlib.Path
        Path to the downloaded ``.grib2`` file under ``scratchspace``.

    Examples
    --------
    >>> path = download_aifs_ens_slice(cli, date, 24, "tp", product="em")   # ensemble mean
    >>> path = download_aifs_ens_slice(cli, date, 72, "tp", product="ep")   # probability product
    """
    fname = f"{date:%Y%m%d%H}00-{fhour:03d}h-enfo-{product}-{param}.grib2"
    target = scratchspace / fname
    if not force and target.exists():
        eprint(f"✓ {target.name} already exists")
        return target

    eprint(f"⇣ Downloading {target.name}")
    try:
        cli.retrieve(
            date=date.isoformat(),
            time=0,
            stream="enfo",
            type=product,
            step=fhour,
            param=param,
            target=str(target),
        )
    except requests.HTTPError as http_err:
        if getattr(http_err.response, "status_code", None) == 404:
            msg = f"✗ Not found (404) for {product=} {param=} {fhour=} {date=}."
            raise DataNotAvailableError(msg) from http_err
        raise (http_err)
    except Exception as e:
        eprint(f"✗ Failed to download {target.name}.\n{e}")
        raise e

    eprint(f"✓ Downloaded {target.name}!")
    return target


def grib_to_xarray(path: Path, param: str) -> xr.DataArray:
    """
    Open a GRIB file and return a cleaned, latitude/longitude-sorted DataArray.

    - Works across parameters (e.g., "tp", "2t", "10u", "10v", …).
    - Sorts `latitude` and `longitude` ascending.
    - Normalizes/cleans coords to avoid collisions with later `expand_dims`:
      drops scalar `step` if present; renames scalar `time` -> `valid_time`.
    - Adds minimal, consistent attributes.
    """
    ds = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": param}, "indexpath": ""},
        decode_timedelta=True,
    )

    da = ds[param]
    da = da.rename({"time": "forecast_reference_time"})
    da = da.reset_coords(["valid_time"], drop=True)
    da = da.sortby(["latitude", "longitude"])

    # Attach tidy attrs (preserve units/long_name when present)
    src_attrs = ds[param].attrs
    da = da.assign_attrs(
        {
            "long_name": src_attrs.get("long_name")
            or src_attrs.get("GRIB_name")
            or param,
            "units": src_attrs.get("units"),
            "param_short_name": param,
            "source": "ECMWF AIFS-ENS Open Data",
            "Conventions": "CF-1.10",
        }
    )
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

    # group by FRT
    groups: dict[int, list[xr.DataArray]] = {}
    for da in arrays:
        da = da.transpose("forecast_reference_time", "step", "latitude", "longitude")
        frt = da["forecast_reference_time"].values.item()
        groups.setdefault(frt, []).append(da)

    # order by FRT, and within each group order by step
    frt_keys = sorted(groups.keys())
    nested = [sorted(groups[frt], key=lambda d: d["step"].values.item()) for frt in frt_keys]

    ds = xr.combine_nested(
        nested,
        concat_dim=["forecast_reference_time", "step"],
        coords="minimal",
        join="exact",
        combine_attrs="drop_conflicts"
    )
    ds = ds.to_dataset(name=dataset_name).chunk(CHUNKING)

    # 3) Data-variable encodings
    for var in ds.data_vars:
        ds[var].encoding.update(
            {
                "chunks": tuple(CHUNKING[dim] for dim in ds[var].dims),  # type: ignore
                "compressors": [COMPRESSOR],
                "_FillValue": np.nan,
            }
        )

    # Coordinate encodings: write each axis as one chunk to minimize GETs
    for coord in ("forecast_reference_time", "step", "latitude", "longitude"):
        if coord in ds.coords:
            ds[coord].encoding["chunks"] = (AXIS_CHUNK,)

    eprint(f"✓ Standardised {dataset_name} dataset in {time.time() - start:.2f}s")
    return ds
