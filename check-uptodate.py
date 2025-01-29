import subprocess
import sys
from datetime import datetime
from pathlib import Path

import click
from typing_extensions import get_args

from etl_scripts.grabbag import eprint

print_ds_path = (Path(__file__).parent / "print-ds.py").resolve()


@click.command
@click.argument(
    "etl-script-path",
    type=click.Path(exists=True, readable=True, resolve_path=True, path_type=Path),
)
@click.argument("dataset")
@click.argument("cid", type=str)
def main(etl_script_path: Path, dataset: str, cid: str):
    get_available_timespan = subprocess.run(
        [
            "uv",
            "run",
            etl_script_path,
            "get-available-timespan",
            dataset,
        ],
        capture_output=True,
        text=True,
    )
    if get_available_timespan.returncode != 0:
        eprint("Could not get available timespan, process returned nonzero exit code")
        sys.exit(1)
    # Only keep the latest date string, which is the second one
    _, d_str = get_available_timespan.stdout.rstrip().split()
    source_latest = datetime.fromisoformat(d_str)
    eprint("Latest timestamp available from the source is")
    print(source_latest)

    ipfs_latest_date = subprocess.run(
        [
            "uv",
            "run",
            print_ds_path,
            "ipfs",
            cid,
            "--print-latest-timestamps",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    if ipfs_latest_date.returncode != 0:
        eprint(
            "Could not get latest date from ipfs with print-ds.py, process returned nonzero exit code"
        )
        sys.exit(1)
    ipfs_latest = datetime.fromisoformat(ipfs_latest_date.stdout.rstrip())
    eprint("Latest timestamp on ipfs is")
    print(ipfs_latest)

    if source_latest > ipfs_latest:
        eprint("Source is ahead of data on ipfs, exiting with error status code 1")
        sys.exit(1)


if __name__ == "__main__":
    main()
