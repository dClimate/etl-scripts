"""
This script writes a kerchunk multizarr to the destination folder by combining all .nc.json found in the dataset's folder to single Zarr kerchunk files.
"""

from pathlib import Path
import re

from kerchunk.combine import MultiZarrToZarr
from msgspec import json


def combine_and_write_multizarr_json(
    single_zarr_jsons: list[Path], destination_dir: Path, fill_value: float | None
):
    print(f"Combining the following into a MultiZarr: {single_zarr_jsons}")
    json_paths = list(map(str, single_zarr_jsons))

    # Only do fill value preprocessing if a fill value is actually specified
    if fill_value is not None:

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


if __name__ == "__main__":
    import sys

    def print_usage():
        script_call_path = sys.argv[0]
        print(f"Usage: python {script_call_path} <provider> <dataset>")
        print(f"Example: python {script_call_path} cpc precip-conus")

    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 2:
        print(
            f"Error: Script did not receive only two arguments, was provided {num_arguments} arguments"
        )
        print_usage()
        sys.exit(1)

    # Verify that the first argument is one of the valid dataset providers
    data_provider = sys.argv[1]
    match data_provider:
        case "cpc":
            pass
        case "chirps" | "prism":
            print(f"Data provider {data_provider} not supported yet")
            sys.exit(1)
        case _:
            print(f"Invalid data provider argument {data_provider}")
            print_usage()
            sys.exit(1)

    # Verify that the second argument is one of the valid datasets in that provider
    dataset = sys.argv[2]
    match data_provider:
        case "cpc":
            match dataset:
                case "precip-conus" | "precip-global" | "tmax" | "tmin":
                    pass
                case _:
                    print(f"Invalid dataset {dataset} for provider {data_provider}")
                    print_usage()
                    sys.exit(1)
        case "chirps" | "prism":
            print(f"Data provider {data_provider} not supported yet")
            sys.exit(1)

    # Get all the .nc.jsons we need
    current_file_dir = Path(__file__).parent
    # The .resolve() removes the ".." from the final path
    dataset_dir = (current_file_dir / ".." / data_provider / dataset).resolve()
    source_json_paths = list(dataset_dir.glob("*.nc.json"))
    if len(source_json_paths) == 0:
        print(f"No .nc.json files found in {dataset_dir}")
        print("Quitting combining")
        sys.exit(1)

    # Call the script functions
    combine_and_write_multizarr_json(
        source_json_paths, dataset_dir, fill_value=-9.96921e36
    )
