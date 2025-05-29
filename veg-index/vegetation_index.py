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

import sys
from datetime import datetime
from typing import Iterable, List

import asyncclick as click
import numpy as np
import xarray as xr
from fpar import download_tiff, tiff_to_dataarray, yield_dekad_dates
from fpar_minmax import MAX_PATH, MIN_PATH, get_dekad_index
from multiformats import CID
from py_hamt import ZarrHAMTStore
from utils import ipfs_hamt_store, standardise

from etl_scripts.grabbag import eprint, npdt_to_pydt

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
    fpar = tiff_to_dataarray(download_tiff(ts, force=force))

    # 2. Select historical min/max for the *same* dekad index
    dekad_idx = get_dekad_index(ts)
    fmin = min_arr.isel(dekad=dekad_idx).load()
    fmax = max_arr.isel(dekad=dekad_idx).load()

    # 3. Compute VCI with protection against divide‑by‑zero
    denom = np.where(fmax - fmin == 0, np.nan, fmax - fmin)
    vci = ((fpar - fmin) / denom).clip(0.0, 1.0)

    # 4. Expand to 3‑D and cast for compact storage
    return vci.expand_dims(time=[np.datetime64(ts, "ns")]).astype("float32")


def _flush_batch(batch: List[xr.DataArray], dest: ZarrHAMTStore, first: bool) -> None:
    """Write a **batch** of VCI slices to the destination Zarr in one call.

    Parameters
    ----------
    batch
        List of individual dekad slices created by :func:`_make_vci_slice`.
        The list is mutated only inside this function (cleared after write).
    dest
        Target HAMT‑backed Zarr store opened for writing.
    first
        ``True`` for the **very first** write into an empty store – triggers
        ``mode='w'`` so that Zarr metadata are created.  ``False`` for
        subsequent writes, in which case the data are appended along the
        *time* dimension with ``append_dim='time'``.
    """

    ds = standardise(xr.concat(batch, dim="time").to_dataset(name="VCI"))
    mode_kwargs = {"mode": "w"} if first else {"append_dim": "time"}
    ds.to_zarr(store=dest, zarr_format=3, **mode_kwargs)


def _emit_vci_slices(
    dates: Iterable[datetime],
    dest: ZarrHAMTStore,
    force: bool,
    batch_size: int,
    already_have: datetime | None = None,
) -> None:
    """Generate VCI rasters for *dates* and stream them to *dest* in batches.

    The function wraps the repetitive logic of

    * opening the local *FPARmin* / *FPARmax* stores,
    * skipping dekads that are already present in *dest*,
    * grouping new dekads into ``batch_size`` slabs so that each Zarr chunk is
      written **exactly once** (crucial for IPFS performance), and
    * flushing each slab via :func:`_flush_batch`.

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
    already_have
        Timestamp of the **latest** dekad already in *dest* (used by
        ``append``).  When *None*, the function assumes the store is empty and
        the first write will create it.
    """

    if not (MIN_PATH.exists() and MAX_PATH.exists()):
        eprint("✖  min.zarr / max.zarr missing – run fpar-minmax.py first.")
        sys.exit(1)

    min_arr = xr.open_zarr(MIN_PATH)["FPARmin"]
    max_arr = xr.open_zarr(MAX_PATH)["FPARmax"]

    first_write = already_have is None
    batch: List[xr.DataArray] = []

    for ts in dates:
        if already_have and ts <= already_have:
            # Dekad already present – skip.
            continue

        batch.append(_make_vci_slice(ts, min_arr, max_arr, force))

        if len(batch) == batch_size:
            _flush_batch(batch, dest, first_write)
            first_write = False
            batch.clear()

    # Flush any remainder (last partial batch)
    if batch:
        _flush_batch(batch, dest, first_write)


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
            yield_dekad_dates(start_date, end_date),
            store,
            force,
            batch_size=batch_size,
            already_have=None,
        )

    eprint("✓ VCI build complete. HAMT CID:")
    print(hamt.root_node_id)


@cli.command("append")
@click.argument("vci_cid")
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
async def append(
    vci_cid: str,
    start_date: datetime,
    end_date: datetime,
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
        latest_vci = npdt_to_pydt(xr.open_zarr(store).time[-1].values)

        _emit_vci_slices(
            yield_dekad_dates(start_date, end_date),
            store,
            force,
            batch_size=batch_size,
            already_have=latest_vci,
        )

    eprint("✓ Append done. New HAMT CID:")
    print(hamt.root_node_id)


if __name__ == "__main__":
    cli()
