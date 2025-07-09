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

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import click
import numpy as np
import time
import zarr
import zarr.codecs
import xarray as xr
from utils import (
    download_tiff,
    scratchspace,
    tiff_to_dataarray,
    yield_dekad_dates,
)

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
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Number of dekads written per to_zarr() call",
)
def generate_minmax(
    start_date: datetime, end_date: datetime, force: bool, batch_size: int
) -> None:
    """Compute/update minima and maxima for dekads in the given interval."""
    # ── create stores on first run ───────────────────────────────────
    if not MIN_PATH.exists() or not MAX_PATH.exists():
        eprint("Creating fresh min.zarr / max.zarr stores…")
        grid_da = tiff_to_dataarray(download_tiff(start_date))
        lat, lon = len(grid_da.latitude), len(grid_da.longitude)
        _ensure_store(MIN_PATH, lat, lon, "FPARmin")
        _ensure_store(MAX_PATH, lat, lon, "FPARmax")

    min_arr = zarr.open_array(MIN_PATH / "FPARmin", mode="r+")
    max_arr = zarr.open_array(MAX_PATH / "FPARmax", mode="r+")

    min_meta = zarr.open_group(MIN_PATH).attrs
    max_meta = zarr.open_group(MAX_PATH).attrs
    processed = set(min_meta.get("processed_dekads", [])) & set(
        max_meta.get("processed_dekads", [])
    )

    # Define wrapper with a single argument so it can be mapped in the executor
    def _process_tiff_file(ts: datetime) -> xr.DataArray:
        eprint(f"⇢ Processing dekad {ts.strftime('%Y-%m-%d')} …")
        tiff = download_tiff(ts, force=force)
        da = tiff_to_dataarray(tiff)
        return da

    # ── main loop ────────────────────────────────────────────────────
    dates = list(yield_dekad_dates(start_date, end_date))
    dates = [d for d in dates if d.strftime("%Y-%m-%d") not in processed]
    eprint(f"Found {len(dates)} dekads to process between {start_date} and {end_date}.")

    # Compute and group the dates by dekad index
    dekad_indices = [get_dekad_index(ts) for ts in dates]
    dekad_groups = {i: [] for i in range(NUM_DEKADS)}
    for ts, idx in zip(dates, dekad_indices):
        dekad_groups[idx].append(ts)

    for idx, slab in dekad_groups.items():
        if not slab:
            continue

        start = time.time()
        eprint(f"Processing dekads with index {idx} ({len(slab)} dekads)")

        # Download and process all TIFF in parallel
        with ThreadPoolExecutor() as executor:
            arrays = executor.map(_process_tiff_file, slab)
        das = xr.concat(arrays, dim="batch")
        eprint(f"✓ Processed {len(slab)} dekads in {time.time() - start:.2f}s")

        # merge with the store slice and write back
        eprint(f"⇢ Merging dekads with index {idx} into store arrays …")

        start = time.time()
        new_min = das.min("batch", skipna=True)
        cur_min = xr.DataArray(min_arr[idx], coords=new_min.coords, dims=new_min.dims)
        min_arr[idx] = xr.ufuncs.fmin(cur_min, new_min).values
        eprint(
            f"⇢ Merged dekads with index {idx} into min array in {time.time() - start:.2f}s"
        )

        start = time.time()
        new_max = das.max("batch", skipna=True)
        cur_max = xr.DataArray(max_arr[idx], coords=new_max.coords, dims=new_max.dims)
        max_arr[idx] = xr.ufuncs.fmax(cur_max, new_max).values
        eprint(
            f"⇢ Merged dekads with index {idx} into max array in {time.time() - start:.2f}s"
        )

        start = time.time()
        eprint(f"⇢ Updating metadata for min & max arrays index {idx}")
        tags = set(ts.strftime("%Y-%m-%d") for ts in slab)
        for meta in (min_meta, max_meta):
            meta["processed_dekads"] = sorted(
                set(meta.get("processed_dekads", [])) | tags
            )
        eprint(
            f"⇢ Updated metadata for dekads with index {idx} in {time.time() - start:.2f}s"
        )

    # ── finalise stores ───────────────────────────────────────────────
    eprint("Consolidating metadata …")
    zarr.consolidate_metadata(MIN_PATH)
    zarr.consolidate_metadata(MAX_PATH)

    eprint("✓ Finished min.zarr / max.zarr build.")


if __name__ == "__main__":
    cli()
