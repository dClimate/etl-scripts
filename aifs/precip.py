#!/usr/bin/env python3
"""
precip.py — AIFS-ENS precipitation ETL

Usage examples
--------------

# Create a brand-new archive containing 2025-07-14 06 UTC run, steps 0-24 h
python precip.py instantiate \
       --run-date 2025-07-14T06:00 --max-step 24 --out zarr://my/precip.zarr

# Append the most recent run that is available on the server
python precip.py append --cid bafy... --out zarr://my/precip.zarr
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncclick as click
import dask
import numpy as np
import xarray as xr
from utils import (
    aifs_client,
    download_tprate_slice,
    grib_to_xarray,
    standardise,
)

dask.config.set(scheduler="threads", num_workers=os.cpu_count())


@click.group()
def cli() -> None:
    pass


@cli.command("instantiate")
@click.argument("start_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("end_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True, help="Redownload even if file exists.")
async def instantiate(
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,

    run_date: datetime, max_step: int, out: str) -> None:
    """Download one full forecast run (all steps 0…max_step) into a Zarr store."""
    cli = aifs_client()
    steps = list(range(0, max_step + 6, 6))

    # 1) download all lead times in parallel
    def _worker(fhr: int) -> xr.DataArray:
        path = download_tprate_slice(cli, run_date, fhr, product="pf")
        da   = grib_to_xarray(path)
        ts   = np.datetime64(run_date + timedelta(hours=fhr), "ns")
        return da.expand_dims(time=[ts])

    t0 = time.time()
    with ThreadPoolExecutor() as pool:
        arrays = list(pool.map(_worker, steps))
    print(f"✓ Downloaded {len(arrays)} slices in {time.time() - t0:.1f}s")

    # 2) concatenate & store
    ds = standardise(arrays, dataset_name="AIFS-Precip")
    ds.to_zarr(out, mode="w", zarr_format=3)
    print(f"✓ Wrote archive to {out}")




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
            ds.to_zarr(store=store, zarr_format=3, **mode_kwargs)
            eprint(
                f"✓ Wrote dekads {slab[0].date()} → {slab[-1].date()} in {time.time() - start:.2f}s"
            )

    eprint("✓ Done. Final HAMT CID:")
    print(hamt.root_node_id)



if __name__ == "__main__":
    cli()
