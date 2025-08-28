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

import itertools
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import asyncclick as click
import dask
import numpy as np
import xarray as xr
from dask.diagnostics.progress import ProgressBar
from ecmwf.opendata import Client
from multiformats import CID
from utils import aifs_client, download_aifs_ens_slice, grib_to_xarray, standardise

from etl_scripts.grabbag import eprint, npdt_to_pydt
from etl_scripts.hamt_store_contextmanager import ipfs_hamt_store

dask.config.set(scheduler="threads", num_workers=os.cpu_count())

SUPPORTED_VARIABLES: dict[str, tuple[str, str]] = {
    "precipitation": ("tp", "AIFS-ensembles Precipitation"),
    "temperature": ("2t", "AIFS-ensembles 2m Temperature"),
    "windU": ("10u", "AIFS-ensembles 10m Wind U"),
    "windV": ("10v", "AIFS-ensembles 10m Wind V"),
    "soilMoisture": ("swvl1", "AIFS-ensembles Soil Moisture L1"),
}

@click.group()
def cli() -> None:
    pass


def download_and_process_data(
    client: Client,
    param: str,
    force: bool,
    date_with_hour: tuple[datetime, int]
) -> xr.DataArray:
    date, hour = date_with_hour
    path = download_aifs_ens_slice(
        cli=client,
        date=date,
        fhour=hour,
        param=param,
        product="cf",
        force=force,
    )
    da = grib_to_xarray(path, param)
    return da.expand_dims(
        forecast_reference_time=[np.datetime64(date, "ns")],
        step=[np.timedelta64(hour, "h")],
    )


def _build_times_and_steps_list(
    start_date: datetime, end_date: datetime
) -> list[tuple[datetime, int]]:
    hours = range(0, 360, 6)
    dates: list[datetime] = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=1)
    return list(itertools.product(dates, hours))


@cli.command("instantiate")
@click.argument("start_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("end_date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("variable", type=click.Choice(tuple(SUPPORTED_VARIABLES.keys())))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True, default=False, help="Redownload file.")
async def instantiate(
    start_date: datetime,
    end_date: datetime,
    variable: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,
) -> None:
    """Download one full forecast run (all steps 0…max_step) into a Zarr store."""
    client = aifs_client()
    param, dataset_name = SUPPORTED_VARIABLES[variable]

    def _worker(date_with_hour: tuple[datetime, int]) -> xr.DataArray:
        return download_and_process_data(client, param, force, date_with_hour)

    # AIFS data is available up to 2 days in the past
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if start_date < today - timedelta(days=2):
        raise ValueError("start_date must be at most 2 days in the past")

    dates_with_hours = _build_times_and_steps_list(start_date, end_date)
    async with ipfs_hamt_store(gateway_uri_stem, rpc_uri_stem) as (store, hamt):
        # 1) download all lead times in parallel
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as pool:
            arrays = list(pool.map(_worker, dates_with_hours))
        eprint(f"✓ Downloaded {len(arrays)} slices in {time.time() - t0:.1f}s")

        ds = standardise(arrays, dataset_name=dataset_name)

        # 2) concatenate & store
        with ProgressBar():
            ds.to_zarr(store=store, mode="w", zarr_format=3)
        eprint(f"✓ Wrote archive {dates_with_hours[0]} → {dates_with_hours[-1]}")

    eprint("✓ Done. Final HAMT CID:")
    print(hamt.root_node_id)


@cli.command("append")
@click.argument("cid")
@click.argument("variable", type=click.Choice(tuple(SUPPORTED_VARIABLES.keys())))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--gateway-uri-stem")
@click.option("--rpc-uri-stem")
@click.option("--force", is_flag=True)
async def append(
    cid: str,
    end_date: datetime | None,
    variable: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    force: bool,
) -> None:
    """Extend an existing IPFS AIFS Zarr with all available dekads up to *end_date*."""

    client = aifs_client()
    param, dataset_name = SUPPORTED_VARIABLES[variable]

    def _worker(date_with_hour: tuple[datetime, int]) -> xr.DataArray:
        return download_and_process_data(client, param, force, date_with_hour)

    # ── open the existing store ──────────────────────────────────────────────
    async with ipfs_hamt_store(
        gateway_uri_stem, rpc_uri_stem, root_cid=CID.decode(cid)
    ) as (store, hamt):
        latest = npdt_to_pydt(xr.open_zarr(store=store).time[-1].values)
        start_date = latest + timedelta(days=1)
        end_date = end_date or datetime.now(UTC)

        # AIFS data is available up to 2 days in the past
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if start_date == today:
            eprint("✓ No new dekads to append.")
        elif start_date < today - timedelta(days=2):
            eprint(
                "We only have access to the last 3 days of data. >> The dataset will have a gap! <<"
            )
            start_date = today - timedelta(days=2)

        dates_with_hours = _build_times_and_steps_list(start_date, end_date)

        # 1) download all lead times in parallel
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as pool:
            arrays = list(pool.map(_worker, dates_with_hours))
        eprint(f"✓ Downloaded {len(arrays)} slices in {time.time() - t0:.1f}s")

        ds = standardise(arrays, dataset_name=dataset_name)

        # 2) concatenate & store
        with ProgressBar():
            ds.to_zarr(
                store=store,
                zarr_format=3,
                append_dim="forecast_reference_time",
                align_chunks=True,
            )
        eprint(f"✓ Wrote archive {dates_with_hours[0]} → {dates_with_hours[-1]}")

    eprint("✓ Done. New HAMT CID:")
    print(hamt.root_node_id)


if __name__ == "__main__":
    cli()
