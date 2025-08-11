#!/usr/bin/env python3
"""fpar_etl.py — FPAR Vegetation Index ETL

Download, transform and publish **Fraction of Photosynthetically Active
Radiation** (FPAR) dekadal composites from the *Global Agricultural Production
Hotspots* programme as a chunked Zarr backed by IPFS.

The raw data are provided as GeoTIFF rasters at ~500 m (original GSD)
accessible under the following deterministic URL template::

    https://agricultural-production-hotspots.ec.europa.eu/data/
        indicators_fpar/fpar/fpar_YYYYMMDD.tif

where *YYYYMMDD* is the **first day of the dekad** (1, 11 or 21).

Two high-level sub-commands are exposed:

* ``instantiate`` — build a brand-new Zarr archive for a given date range.
* ``append``      — extend an existing archive with more recent dekads.

The resulting content-addressed store can be consumed with ordinary Zarr
front-ends (dask, kerchunk, xarray, rechunker…) without any IPFS-specific glue.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import asyncclick as click
import dask
import numpy as np
import requests
import xarray as xr
from dask.diagnostics.progress import ProgressBar
from multiformats import CID
from utils import (
    CHUNKING,
    download_tiff,
    quality_check_dataset,
    standardise,
    tiff_to_dataarray,
    tiff_url,
    yield_dekad_dates,
)

from etl_scripts.grabbag import eprint, npdt_to_pydt
from etl_scripts.hamt_store_contextmanager import ipfs_hamt_store

dask.config.set(scheduler="threads", num_workers=os.cpu_count())

# -----------------------------------------------------------------------------#
# Command-line interface
# -----------------------------------------------------------------------------#


@click.group()
def cli() -> None:
    """Entry-point for the **fpar-etl** CLI."""


@cli.command("get-available-timespan")
def get_available_timespan() -> None:
    """Probe the source and print *earliest* and *latest* dekad available.

    The service publishes data from 2003-01-01 onwards. The *earliest* boundary
    is therefore hard-coded while the *latest* is determined dynamically by
    probing backwards in 10-day steps until a successful HTTP *HEAD* is
    observed.  The procedure avoids downloading any full TIFFs and finishes in
    O(*n* dekads) where *n* ≈ 90 for a two-year sliding window.
    """

    earliest = "2003-01-01"
    probe = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, day=21)

    for _ in range(90):  # safety: do not probe more than ~2.5 years back
        url = tiff_url(probe)
        try:
            if requests.head(url, timeout=15).ok:
                print(f"{earliest} {probe.strftime('%Y-%m-%d')}")
                return
        except requests.RequestException:
            pass
        probe -= timedelta(days=10)

    eprint("Could not determine latest available date – aborting.")
    sys.exit(1)


@cli.command("download")
@click.argument("timestamp", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--force", is_flag=True)
def download(timestamp: datetime, force: bool) -> None:
    """Fetch a single dekad GeoTIFF to *scratchspace*."""
    path = download_tiff(timestamp, force=force)
    eprint(f"✓ Saved to {path}")


@cli.command("instantiate")
@click.argument("start_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("end_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True, help="Redownload even if file exists.")
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=CHUNKING["time"],
    show_default=True,
    help="Number of dekads written per to_zarr() call",
)
async def instantiate(
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,
    batch_size: int,
) -> None:
    """Create a **new** IPFS-backed Zarr store covering *start_date* → *end_date*.

    Dekads are streamed in *batch_size* slabs so that each Zarr chunk is written
    exactly once, avoiding the read-modify-write penalty of per-dekad appends.
    """

    # ── collect target timestamps ────────────────────────────────────────────
    dates = list(yield_dekad_dates(start_date, end_date))
    if not dates:
        eprint("Nothing to do — no dekads in requested range.")
        return

    # Define wrapper with a single argument so it can be mapped in the executor
    def _process_tiff_file(ts: datetime) -> xr.DataArray:
        tiff = download_tiff(ts, force=force)
        da = tiff_to_dataarray(tiff)
        da = da.expand_dims(time=[np.datetime64(ts, "ns")])
        return da

    async with ipfs_hamt_store(gateway_uri_stem, rpc_uri_stem) as (store, hamt):
        # ── process in contiguous batches ────────────────────────────────────────
        for i in range(0, len(dates), batch_size):
            slab = dates[i : i + batch_size]

            # Download and process all TIFF in parallel
            start = time.time()
            with ThreadPoolExecutor() as executor:
                arrays = list(executor.map(_process_tiff_file, slab))
            eprint(f"✓ Downloaded {len(slab)} dekads in {time.time() - start:.2f}s")

            ds = standardise(arrays, dataset_name="FPAR")
            quality_check_dataset(
                ds, raw_arrays=dict(zip(slab, arrays)), dataset_name="FPAR"
            )

            start = time.time()
            eprint(f"Writing dekads {slab[0].date()} → {slab[-1].date()}…")
            mode_kwargs = {"mode": "w"} if i == 0 else {"append_dim": "time"}

            with ProgressBar():
                ds.to_zarr(store=store, zarr_format=3, **mode_kwargs)

            eprint(
                f"✓ Wrote dekads {slab[0].date()} → {slab[-1].date()} in {time.time() - start:.2f}s"
            )

    eprint("✓ Done. Final HAMT CID:")
    print(hamt.root_node_id)


@cli.command("append")
@click.argument("cid")
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=CHUNKING["time"],
    show_default=True,
    help="Dekads written per to_zarr() call",
)
async def append(
    cid: str,
    end_date: datetime | None,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,
    batch_size: int,
) -> None:
    """Extend an existing IPFS Zarr with all available dekads up to *end_date*.

    Dekads are processed in *batch_size* slabs so that each Zarr chunk is
    created only once, eliminating the costly read-modify-write pattern.
    """

    # Define wrapper with a single argument so it can be mapped in the executor
    def _process_tiff_file(ts: datetime) -> xr.DataArray:
        tiff = download_tiff(ts, force=force)
        da = tiff_to_dataarray(tiff)
        da = da.expand_dims(time=[np.datetime64(ts, "ns")])
        return da

    # ── open the existing store ──────────────────────────────────────────────
    async with ipfs_hamt_store(
        gateway_uri_stem, rpc_uri_stem, root_cid=CID.decode(cid)
    ) as (store, hamt):
        latest = npdt_to_pydt(xr.open_zarr(store=store).time[-1].values)
        start_date = latest + timedelta(days=10)  # first dekad NOT in store
        end_date = end_date or datetime.now(UTC)

        dates = list(yield_dekad_dates(start_date, end_date))
        if not dates:
            eprint("✓ No new dekads to append.")
            return

        # ── process in contiguous batches ───────────────────────────────────────
        for i in range(0, len(dates), batch_size):
            slab = dates[i : i + batch_size]

            # Download and process all TIFF in parallel
            start = time.time()
            with ThreadPoolExecutor() as executor:
                arrays = list(executor.map(_process_tiff_file, slab))
            eprint(f"✓ Downloaded {len(slab)} dekads in {time.time() - start:.2f}s")

            ds = standardise(arrays, dataset_name="FPAR")
            quality_check_dataset(
                ds, raw_arrays=dict(zip(slab, arrays)), dataset_name="FPAR"
            )

            start = time.time()
            eprint(f"Writing dekads {slab[0].date()} → {slab[-1].date()}…")

            with ProgressBar():
                ds.to_zarr(store=store, zarr_format=3, append_dim="time", align_chunks=True)

            eprint(
                f"✓ Wrote dekads {slab[0].date()} → {slab[-1].date()} in {time.time() - start:.2f}s"
            )

    eprint("✓ Done. New HAMT CID:")
    print(hamt.root_node_id)


if __name__ == "__main__":
    cli()
