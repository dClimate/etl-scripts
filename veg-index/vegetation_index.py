#!/usr/bin/env python3
"""
Vegetation Condition Index (VCI) ETL — TIFF‑driven
=================================================

VCI is a relative measure of plant stress that compares *current* satellite‑
derived **Fraction of Photosynthetically Active Radiation absorbed by
vegetation** (FPAR) with the historical minimum and maximum FPAR for the **same
10‑day period** (a *dekad*). It is defined as

```
VCI = (FPAR − FPAR_min) / (FPAR_max − FPAR_min)
```

* Values close to **0** → vegetation performing near the historical minimum
  for that dekad (potential stress).
* Values close to **1** → vigorous vegetation near the historical maximum.

This script can

* **instantiate** – build a brand‑new time‑series Zarr on IPFS for a given date
  range, or
* **append** – extend an existing store with more recent dekads.

Both operations publish through a content‑addressed HAMT store so the output is
traceable and immutable.
"""

from __future__ import annotations

import gc
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import asyncclick as click
import numpy as np
import xarray as xr
from dask.diagnostics.progress import ProgressBar
from fpar import download_tiff, tiff_to_dataarray, yield_dekad_dates
from fpar_minmax import MAX_PATH, MIN_PATH, get_dekad_index
from multiformats import CID
from py_hamt import ZarrHAMTStore
from utils import quality_check_dataset, standardise

from etl_scripts.grabbag import eprint, npdt_to_pydt
from etl_scripts.hamt_store_contextmanager import ipfs_hamt_store

# ── helpers ──────────────────────────────────────────────────────────


def _make_vci_slice(
    ts: datetime,
    min_arr: xr.DataArray,
    max_arr: xr.DataArray,
    force: bool,
) -> xr.DataArray:
    """Create an **in‑memory** VCI slice for a single dekad.

    Parameters
    ----------
    ts
        UTC‑normalised timestamp marking the **first day** of the dekad.
    min_arr, max_arr
        Lazy *FPARmin* / *FPARmax* arrays opened from the local reference Zarrs
        (dimensions: ``dekad, latitude, longitude``).
    force
        If *True*, redownload the FPAR GeoTIFF even when it is already present
        in the on‑disk cache.

    Returns
    -------
    xarray.DataArray
        3‑D array with dimensions ``(time, latitude, longitude)`` and a single
        element along the *time* axis containing VCI values clipped to
        ``[0, 1]``.
    """

    # 1. Load the current FPAR observation (down‑sampled ≈1 km grid)
    eprint(f"Downloading FPAR for {ts:%Y-%m-%d}…")
    fpar = tiff_to_dataarray(download_tiff(ts, force=force)).chunk(
        {"latitude": 512, "longitude": 512}
    )

    # 2. Select historical min/max for the *same* dekad index
    dekad_idx = get_dekad_index(ts)
    eprint(f"Loading min and max arrays for dekad index {dekad_idx}…")
    fmin = min_arr.isel(dekad=dekad_idx)
    fmax = max_arr.isel(dekad=dekad_idx)

    # 3. Compute VCI with protection against divide‑by‑zero
    eprint(f"Computing VCI {ts:%Y-%m-%d}…")
    denom = xr.where((fmax - fmin) == 0, np.nan, fmax - fmin)
    vci_lazy = ((fpar - fmin) / denom).clip(0.0, 1.0)
    vci = vci_lazy.compute()

    eprint(f"✓ VCI computed {ts:%Y-%m-%d}. Freeing memory…")
    del fmin, fmax, fpar
    gc.collect()

    # 4. Expand to 3‑D and cast for compact storage
    eprint(f"Wrapping VCI in xarray.DataArray for {ts:%Y-%m-%d}…")
    return vci.expand_dims(time=[np.datetime64(ts, "ns")]).astype("float32")


def _emit_vci_slices(
    dates: list[datetime],
    dest: ZarrHAMTStore,
    force: bool,
    batch_size: int,
    is_first_write: bool,
) -> None:
    """Generate VCI rasters for *dates* and stream them to *dest* in batches.

    The function wraps the repetitive logic of

    * opening the local *FPARmin* / *FPARmax* stores,
    * skipping dekads that are already present in *dest*,
    * grouping new dekads into ``batch_size`` slabs so that each Zarr chunk is
      written **exactly once** (crucial for IPFS performance), and
    * flushing each slab via to zarr.

    Parameters
    ----------
    dates
        Iterable of dekad timestamps (e.g. from
        :func:`fpar.yield_dekad_dates`).
    dest
        Destination HAMT‑backed Zarr store (opened in write mode).
    force
        Propagated to :func:`download_tiff`; forces re‑download of source
        GeoTIFFs when *True*.
    batch_size
        Number of dekads to accumulate before each :py:meth:`xarray.Dataset.to_zarr`
        call.  Match this to the *time* chunk for optimal speed.
    is_first_write
        If *True*, the store is empty and the first write will create the
        Zarr metadata.  If *False*, the data are appended along the *time*
        dimension.
    """

    if not (MIN_PATH.exists() and MAX_PATH.exists()):
        eprint("✖  min.zarr / max.zarr missing – run fpar-minmax.py first.")
        sys.exit(1)

    min_arr = xr.open_zarr(MIN_PATH)["FPARmin"]
    max_arr = xr.open_zarr(MAX_PATH)["FPARmax"]

    # Define wrapper with a single argument so it can be mapped in the executor
    def _process_ts(ts: datetime) -> xr.DataArray:
        return _make_vci_slice(ts, min_arr, max_arr, force)

    for i in range(0, len(dates), batch_size):
        slab = dates[i : i + batch_size]
        eprint(f"Processing dekads {slab[0].date()} → {slab[-1].date()}…")
        eprint(f"Timestamp now: {datetime.now(UTC)}")

        # Download and process all TIFF in parallel
        start = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            arrays = list(executor.map(_process_ts, slab))
        eprint(f"✓ Processed {len(slab)} dekads in {time.time() - start:.2f}s")

        ds = standardise(arrays, dataset_name="VCI")
        quality_check_dataset(
            ds, raw_arrays=dict(zip(slab, arrays)), dataset_name="VCI"
        )
        del arrays
        gc.collect()

        start = time.time()
        eprint(f"Writing dekads {slab[0].date()} → {slab[-1].date()}…")
        mode_kwargs = {"mode": "w"} if is_first_write else {"append_dim": "time"}

        with ProgressBar():
            ds.to_zarr(store=dest, zarr_format=3, **mode_kwargs)

        is_first_write = False  # After the first write, we append
        eprint(
            f"✓ Wrote dekads {slab[0].date()} → {slab[-1].date()} in {time.time() - start:.2f}s"
        )


# ── CLI ————————————————————————————————————————————————————————————
@click.group()
def cli() -> None:
    """*vci‑etl* command‑line interface.

    Sub‑commands
    ------------
    instantiate
        Build a brand‑new VCI Zarr and publish to IPFS.
    append
        Add further dekads to an existing store.
    """


@cli.command("instantiate")
@click.argument("start_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("end_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Dekads written per to_zarr() call (should equal the time chunk).",
)
async def instantiate(
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,
    batch_size: int,
):
    """Create a **new** VCI Zarr covering *start_date* → *end_date*.

    The routine streams through all dekads in the requested range, computes
    VCI, uploads the resulting Zarr to IPFS, and prints the *root HAMT CID* to
    **stdout**.  Capture that CID for future *append* operations.
    """

    async with ipfs_hamt_store(gateway_uri_stem, rpc_uri_stem) as (store, hamt):
        _emit_vci_slices(
            list(yield_dekad_dates(start_date, end_date)),
            store,
            force,
            batch_size=batch_size,
            is_first_write=True,
        )

    eprint("✓ VCI build complete. HAMT CID:")
    print(hamt.root_node_id)


@cli.command("append")
@click.argument("vci_cid")
@click.argument("end_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Dekads written per to_zarr() call (should equal the time chunk).",
)
async def append(
    vci_cid: str,
    end_date: datetime | None,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,
    batch_size: int,
):
    """Extend an **existing** VCI Zarr with new dekads.

    Any dekads already present in the store are automatically skipped.  On
    success the *new* HAMT CID is printed (it may change due to shard
    re‑packing).
    """

    async with ipfs_hamt_store(
        gateway_uri_stem, rpc_uri_stem, root_cid=CID.decode(vci_cid)
    ) as (store, hamt):
        latest = npdt_to_pydt(xr.open_zarr(store=store).time[-1].values)
        start_date = latest + timedelta(days=10)  # first dekad NOT in store
        end_date = end_date or datetime.now(UTC)
        dates = list(yield_dekad_dates(start_date, end_date))

        _emit_vci_slices(
            dates, store, force, batch_size=batch_size, is_first_write=False
        )

    eprint("✓ Append done. New HAMT CID:")
    print(hamt.root_node_id)


if __name__ == "__main__":
    cli()
