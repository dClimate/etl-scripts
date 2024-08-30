"""
This script takes a list of zarr JSONs, and a destination folder, then writes a kerchunk multizarr to the destination folder
"""

from pathlib import Path
from kerchunk.combine import MultiZarrToZarr
from msgspec import json

from typing import List


def combine_and_write_multizarr_json(
    single_zarr_jsons: List[Path], destination_dir: Path
):
    print(f"Combining the following into a MultiZarr: {single_zarr_jsons}")
    json_paths = list(map(str, single_zarr_jsons))

    multizarr = MultiZarrToZarr(json_paths, concat_dims="time")
    multizarr_dict = multizarr.translate()

    multizarr_destination = destination_dir / f"{destination_dir.name}.json"
    print(f"Writing Multizarr JSON to {multizarr_destination}")
    with open(multizarr_destination, "wb") as f:
        f_bytes = json.encode(multizarr_dict)
        f.write(f_bytes)


if __name__ == "__main__":
    import sys

    script_call_path = sys.argv[0]

    def print_usage():
        print(
            f"Usage: python {script_call_path} zarr_json_path1... multizarr_destination"
        )
        print(
            f"Example: python {script_call_path} 2007.json 2008.json cpc/precip-conus"
        )

    # Make sure there is at least one source json and one destination folder
    num_arguments = len(sys.argv) - 1
    if num_arguments < 2:
        print(
            f"Error: Script received less than one argument, was provided {num_arguments} arguments"
        )
        print_usage()
        sys.exit(1)

    # Verify that the first set of arguments are all kerchunk zarr JSONs that exist
    # Get all arguments from 2nd to 2nd to last
    source_json_args = sys.argv[1:-1]
    source_json_paths = list(map(Path, source_json_args))
    for p in source_json_paths:
        if not p.is_file():
            print(f"Error: Argument {p} is not a file that exists")
            print_usage()
            sys.exit(1)
    # TODO verify that suffix is json

    # Verify that the last argument is a directory that exists
    last_argument = sys.argv[len(sys.argv) - 1]
    destination = Path(last_argument)
    if not destination.is_dir():
        print(
            f"Error: Script received a destination directory that does not exist: {last_argument}"
        )
        print_usage()
        sys.exit(1)

    # Call the script functions
    combine_and_write_multizarr_json(source_json_paths, destination)
