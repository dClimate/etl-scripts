"""
This script writes a kerchunk multizarr to the destination folder by combining all .nc.json found in the dataset's folder to single Zarr kerchunk files.
"""

from pathlib import Path
import re
import sys

from kerchunk.combine import MultiZarrToZarr
from msgspec import json


def generate_fix_fill_values_preprocessor(fill_value):
    def fix_fill_values_preprocessor(refs):
        ref_names = set()
        file_match_pattern = (
            r"(.*?)/"  # only modify the Zarr files with a period at the beginning
        )
        for ref in refs:
            match = re.match(file_match_pattern, ref)
            if match:
                ref_names.add(match.group(1))

        for ref in ref_names:
            # Replace the fill value in the zarr metadata
            zarray_dict = json.decode(refs[f"{ref}/.zarray"])
            zarray_dict["fill_value"] = fill_value
            refs[f"{ref}/.zarray"] = json.encode(zarray_dict)

    return fix_fill_values_preprocessor


def combine_and_write_multizarr_json(
    single_zarr_jsons: list[Path], destination_dir: Path, fill_value: float | None
):
    print(f"Combining the following into a MultiZarr: {single_zarr_jsons}")
    json_paths = list(map(str, single_zarr_jsons))

    # Only do fill value preprocessing if a fill value is actually specified
    if fill_value is not None:
        fix_fill_values_preprocessor = generate_fix_fill_values_preprocessor(fill_value)
        multizarr = MultiZarrToZarr(
            json_paths, concat_dims="time", preprocess=fix_fill_values_preprocessor
        )
    else:
        multizarr = MultiZarrToZarr(json_paths, concat_dims="time")

    multizarr_dict = multizarr.translate()
    multizarr_destination = destination_dir / f"{destination_dir.name}.json"
    with open(multizarr_destination, "wb") as f:
        f_bytes = json.encode(multizarr_dict)
        print(f"Writing Multizarr JSON to {multizarr_destination}")
        f.write(f_bytes)


def print_usage():
    script_call_path = sys.argv[0]
    print(f"Usage: python {script_call_path} <directory with kerchunk .nc.json files>")
    print(f"Example: python {script_call_path} chirps/prelim-p25")


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print("Error: Script received more than one argument")
        print_usage()
        sys.exit(1)

    # Verify the path is a directory
    dataset_dir = Path(sys.argv[1])
    if not dataset_dir.is_dir():
        print(f"Error: Path {dataset_dir} is not a directory")
        sys.exit(1)

    source_json_paths = list(dataset_dir.glob("*.nc.json"))
    if len(source_json_paths) == 0:
        print(f"Error: No .nc.json files found in {dataset_dir}")
        sys.exit(1)

    # Call the script functions
    combine_and_write_multizarr_json(
        source_json_paths, dataset_dir, fill_value=-9.96921e36
    )


if __name__ == "__main__":
    main()
