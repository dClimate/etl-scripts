# cli.py
import json
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Literal
import time
from zarr.storage import MemoryStore
import itertools
import dask.array as da
import subprocess
import warnings

import aioboto3
import boto3
import cdsapi
import click
import numcodecs
import numpy as np
import xarray as xr
import zarr.codecs
from botocore.exceptions import ClientError as S3ClientError
from multiformats import CID
from py_hamt import ShardedZarrStore, KuboCAS
import asyncio
from etl_scripts.grabbag import eprint, npdt_to_pydt
from era5.processor import build_full_dataset, batch_processor, append_latest
from era5.utils import CHUNKER, dataset_names, chunking_settings, time_chunk_size

datasets_choice = click.Choice(dataset_names)
period_options = ["hour", "day", "month"]
period_choice = click.Choice(period_options)


@click.command
@click.argument("dataset", type=datasets_choice)
@click.argument("cid")
@click.option(
    "--end-date",
    type=click.DateTime(),
    required=False,
    help="The target date to append data up to (inclusive). Format: YYYY-MM-DD",
)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc."
)
@click.option("--finalization-only", type=bool, default=True, help="Only take finalization data")
def append(
    dataset,
    cid: str,
    end_date: datetime | None,
    gateway_uri_stem: str,
    rpc_uri_stem: str,
    api_key: str | None,
    finalization_only: bool,
):
    async def main():
        await append_latest( 
            dataset=dataset,
            cid=cid,
            end_date=end_date,
            gateway_uri_stem=gateway_uri_stem,
            rpc_uri_stem=rpc_uri_stem,
            api_key=api_key,
            finalization_only=finalization_only,
        )
    
    asyncio.run(main())


@click.command()
@click.argument("dataset", type=datasets_choice)
@click.option("--start-date",type=click.DateTime(), required=True, help="The first day of the batch to process.")
@click.option("--end-date", type=click.DateTime(), required=True, help="The last day of the batch to process.")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option("--api-key", help="The CDS API key to use.")
def process_batch(
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
):
    """
    Downloads, processes, and creates an IPFS Zarr store for a single batch of data.
    On success, prints the final CID to standard output. All logging goes to stderr.
    This command is intended to be called as a subprocess by the 'append' command.
    """
    async def main():
        batch_cid = await batch_processor(
            dataset=dataset, 
            start_date=start_date, 
            end_date=end_date, 
            gateway_uri_stem=gateway_uri_stem, 
            rpc_uri_stem=rpc_uri_stem, 
            api_key=api_key,
        )
        print(batch_cid)
    asyncio.run(main())

@click.command()
@click.argument("dataset", type=datasets_choice)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option("--api-key", help="The CDS API key to use, as opposed to reading from ~/.cdsapirc.")
@click.option("--max-parallel-procs", type=int, default=1, help="Maximum number of batch processes to run in parallel.")
@click.option("--finalization-only", type=bool, default=True, help="Only take finalization data")
def build_dataset(
    dataset: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    api_key: str | None,
    max_parallel_procs: int,
    finalization_only: bool,
):
    """
    Creates a new, chunk-aligned Zarr store on IPFS with the first 1200 hours (50 days) of ERA5 data,
    then extends it to the latest available timestamp using batch processing. Prints the final CID to stdout.
    All logging goes to stderr.
    """
    asyncio.run(build_full_dataset(    
        dataset=dataset,
        gateway_uri_stem=gateway_uri_stem,
        rpc_uri_stem=rpc_uri_stem,
        api_key=api_key,
        max_parallel_procs=max_parallel_procs,
        finalization_only=finalization_only
    ))

@click.group
def cli():
    """
    Commands for ETLing ERA5. On invocation, a scratch space directory relative to this file will be created for data files at ./scratchspace/era5.
    """
    pass


# cli.add_command(get_available_timespan)
# cli.add_command(download)
# cli.add_command(check_finalized)
# cli.add_command(instantiate)
cli.add_command(append)
cli.add_command(process_batch)
cli.add_command(build_dataset)

if __name__ == "__main__":
    with warnings.catch_warnings():
        # Suppress warnings about consolidated metadata not being part of the store
        # Sharding store doesnt use it
        warnings.filterwarnings("ignore", category=UserWarning, message="Consolidated metadata is currently not part")
        cli()

