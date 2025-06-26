#!/usr/bin/env python3
"""fpar_minmax.py — Incremental per-dekad FPAR extrema

Creates/updates two local Zarr stores:

* scratchspace/vegindex/min.zarr  – per-dekad minima
* scratchspace/vegindex/max.zarr  – per-dekad maxima

Each has shape ``(36, lat, lon)`` where the leading dimension enumerates the
calendar-year dekads:

    Jan-01, Jan-11, Jan-21, Feb-01, …, Dec-21
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import click
import dask
import dask.array as da
import numpy as np
import xarray as xr
import zarr
import zarr.codecs
from dask.diagnostics import ProgressBar
from utils import download_tiff, scratchspace, tiff_to_dataarray, yield_dekad_dates

from etl_scripts.grabbag import eprint

# ── constants ────────────────────────────────────────────────────────
MIN_PATH: Path = scratchspace / "min.zarr"
MAX_PATH: Path = scratchspace / "max.zarr"

NUM_DEKADS = 36  # 3 dekads × 12 months
CHUNKS = (1, 1024, 1024)


# ── helpers ──────────────────────────────────────────────────────────
def get_dekad_index(ts: datetime) -> int:
    """Return a stable 0-based dekad index (0‥35) for *ts*."""
    month = ts.month - 1
    offset = 0 if ts.day == 1 else (1 if ts.day == 11 else 2)
    return month * 3 + offset


def _ensure_store(store_path: Path, lat: int, lon: int, varname: str) -> None:
    """Initialise an **empty** Zarr array schema with xarray-friendly metadata."""
    if store_path.exists():
        return

    zroot = zarr.open_group(store_path, mode="a")
    zroot = zroot.require_array(
        name=varname,
        shape=(NUM_DEKADS, lat, lon),
        chunks=CHUNKS,
        dtype="float32",
        fill_value=np.nan,
        compressor=zarr.codecs.BloscCodec(),
        dimension_names=("dekad", "latitude", "longitude"),
    )

    # Lightweight bookkeeping so incremental runs can skip processed dekads
    zroot.attrs["processed_dekads"] = []


# ── CLI  ──────────────────────────────────────────────────────────────
@click.group()
def cli() -> None:
    """FPAR per-dekad *min/max* builder."""


@cli.command("generate-minmax")
@click.argument("start_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("end_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--force", is_flag=True, help="Re-download existing TIFFs.")
def generate_minmax(start_date: datetime, end_date: datetime, force: bool) -> None:
    """
    Compute / update per-dekad minima & maxima between *start_date* and *end_date*
    using Dask for true parallel execution.
    """

    # ── 1. bootstrap empty Zarr stores if they don’t exist ───────────
    if not MIN_PATH.exists() or not MAX_PATH.exists():
        eprint("Creating fresh min.zarr / max.zarr stores…")
        grid_da = tiff_to_dataarray(download_tiff(start_date))
        lat, lon = len(grid_da.latitude), len(grid_da.longitude)
        _ensure_store(MIN_PATH, lat, lon, "FPARmin")
        _ensure_store(MAX_PATH, lat, lon, "FPARmax")

    # ── 2. open the existing stores *lazily* with one-dekad chunks ───
    #     (shape: time=36 × lat × lon; `time` is “dekad index” 0‥35)
    min_da = xr.open_zarr(MIN_PATH, consolidated=False, chunks={"time": 1})["FPARmin"]
    max_da = xr.open_zarr(MAX_PATH, consolidated=False, chunks={"time": 1})["FPARmax"]

    min_meta = zarr.open_group(MIN_PATH).attrs
    max_meta = zarr.open_group(MAX_PATH).attrs
    already_done = set(min_meta.get("processed_dekads", [])) & set(
        max_meta.get("processed_dekads", [])
    )

    # ── 3. build a *lazy* DataArray for every dekad we still need ────
    def _lazy_da(ts: datetime) -> xr.DataArray:
        """
        Tiny helper that *downloads & decodes inside the Dask task*,
        then returns an xarray.DataArray with:
          * dims  : (time, latitude, longitude)
          * coords: time=<ts>, dekad=<0–35>
        """
        arr = tiff_to_dataarray(download_tiff(ts, force=force))  # eager ndarray
        arr = arr.expand_dims(time=[np.datetime64(ts, "ns")])
        arr = arr.assign_coords(dekad=("time", [get_dekad_index(ts)]))
        # turn the (lat, lon) data *inside* into a Dask array so that
        # the actual TIFF *decode* can run in parallel too
        arr.data = da.from_array(arr.data, chunks=arr.shape)
        return arr

    dates_needed = [
        ts
        for ts in yield_dekad_dates(start_date, end_date)
        if ts.strftime("%Y-%m-%d") not in already_done
    ]

    if not dates_needed:
        eprint("✓ Nothing new to process – stores are up to date.")
        return

    eprint(f"⇢ Processing {len(dates_needed)} dekad(s)…")

    # one delayed task per input TIFF
    lazy_list = [
        xr.apply_ufunc(
            _lazy_da,
            0,
            dask="parallelized",
            input_core_dims=[[]],
            output_dtypes=[np.float32],
            kwargs=dict(ts=ts),
        )
        for ts in dates_needed
    ]

    # concat along time → shape (n_dates, lat, lon), still *lazy*
    stacked = xr.concat(lazy_list, dim="time")

    # ── 4. reduction: min/max by dekad (groupby triggers a Dask graph)
    min_by_dekad = stacked.groupby("dekad").min("time")
    max_by_dekad = stacked.groupby("dekad").max("time")

    # ── 5. merge with existing datasets *without materialising them* ‐
    #       fmin / fmax are ufuncs so apply_ufunc builds another graph
    def _combine(old, new, f):
        # old, new share (time, lat, lon) but `new` usually misses most times
        combined = xr.apply_ufunc(
            f,
            old,
            new,
            dask="parallelized",
            output_dtypes=[old.dtype],
            keep_attrs=True,
        )
        # wherever `new` has NaN (dekads not in range) keep *old* value
        return combined.where(~np.isnan(combined), old)

    updated_min = _combine(min_da, min_by_dekad, np.fmin)
    updated_max = _combine(max_da, max_by_dekad, np.fmax)

    # ── 6. write-back: stream the two dask arrays into their Zarr stores ‐
    #       Xarray’s .to_zarr() can store a *view* with compute=False,
    #       then we trigger everything in one `dask.compute`
    writes = []
    writes.append(
        updated_min.to_zarr(
            MIN_PATH, component="FPARmin", mode="r+", compute=False, consolidated=False
        )
    )
    writes.append(
        updated_max.to_zarr(
            MAX_PATH, component="FPARmax", mode="r+", compute=False, consolidated=False
        )
    )

    # nice progress bar for interactive runs
    with ProgressBar():
        dask.compute(*writes)

    # ── 7. update metadata & consolidate ─────────────────────────────
    new_tags = {ts.strftime("%Y-%m-%d") for ts in dates_needed}
    for meta in (min_meta, max_meta):
        meta["processed_dekads"] = sorted(
            set(meta.get("processed_dekads", [])) | new_tags
        )

    eprint("Consolidating metadata …")
    zarr.consolidate_metadata(MIN_PATH)
    zarr.consolidate_metadata(MAX_PATH)

    eprint("✓ Finished min.zarr / max.zarr build.")


if __name__ == "__main__":
    cli()
