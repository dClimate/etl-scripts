"""
An example using CPC Global Precipitation data.

This example uses the imperative config option, which allows you to construct a pipeline in Python code. For an example
that uses a YAML config file instead of Python code, see the CPC US Precipitation example.
"""

import pathlib

from dc_etl import filespec
from .cli import main
# import fetch from same directory
from .pipeline import Pipeline
from dc_etl import component
from .composite import Composite
from .config import DATASET_CONFIG
from docopt import docopt 
import sys
from .compressor import compress

HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"


def build_pipeline(dataset_type):
    """Builds the pipeline dynamically based on the dataset configuration."""
    
    if dataset_type not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")
    

    config = DATASET_CONFIG[dataset_type]
    # Dynamically load classes based on config
    dataset_values = config["dataset_values_class"]()

    dataset_cache_path = CACHE / dataset_values.dataset_name
    combined_cache_path = dataset_cache_path / "combined"


    assessor = config["assessor_class"](dataset_name=dataset_values.dataset_name, time_resolution=dataset_values.time_resolution)
    transformer = config["transformer_class"]()
    fetcher = config["fetcher_class"](cache=CACHE / dataset_values.dataset_name)
    stac_loader = config["stac_loader_class"](
        time_dim="time", publisher=component.ipld_publisher("local_file", combined_cache_path / "zarr_head.cid"), cache_location=dataset_cache_path
        )
    combiner = config["combiner"]


    return Pipeline(
        assessor=assessor,
        fetcher=fetcher,
        extractor=component.extractor("netcdf"),
        combiner=combiner(
            output_folder=combined_cache_path,
            concat_dims=["time"],
            identical_dims=["longitude", "latitude"],
        ),
        transformer=Composite(
            transformer.dataset_transformer,
            compress([dataset_values.data_var]),
        ),
        loader=stac_loader,
    )

def _parse_args():
    script = sys.argv[0]
    if "/" in script:
        _, script = script.rsplit("/", 1)
    name = script[:-3] if script.endswith(".py") else script
    doc = __doc__.format(name=name, script=script)
    return docopt.docopt(doc)

def mainPipeline(dataset_type):
    """Run the pipeline based on the dataset type."""
    pipeline = build_pipeline(dataset_type)
    main(pipeline)

if __name__ == "__main__":
    usage = """
    ETL Pipeline for {name}

    Usage:
        {script} [options] init
        {script} [options] init-stepped
        {script} [options] append
        {script} [options] replace
        {script} [options] interact

    Options:
        -h --help            Show this screen.
        --timespan SPAN      How much data to load along the time axis. [default: 1M]
        --daterange RANGE    The date range to load.
        --overwrite          Allow data to be overwritten.
        --pdb                Drop into debugger on error.
        --dataset DATASET    The dataset to run (e.g., "sea-surface-height"). [default: sea-surface-height]
    """.format(name="ETL Pipeline", script="etl_pipeline.py")
    
    arguments = docopt(usage)

    dataset_type = arguments['--dataset']
    print(f"Running pipeline for dataset type: {dataset_type}")
    mainPipeline(dataset_type)
