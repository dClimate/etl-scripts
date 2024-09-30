import pathlib
from dc_etl import filespec
from .cli import main

from .pipeline import Pipeline
from dc_etl import component
from .extractor import ERA5NetCDFExtractor
from .combiner import ERA5Combiner
from .composite import Composite
from .compressor import compress
from .config import DATASET_CONFIG
import sys
from docopt import docopt 

HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"

def build_pipeline(dataset_type):
    """Builds the pipeline dynamically based on the dataset configuration."""
    
    if dataset_type not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")
    
    config = DATASET_CONFIG[dataset_type]
    # Dynamically load classes based on config
    dataset_values = config["dataset_values_class"]()
    assessor = config["assessor_class"]()
    transformer = config["transformer_class"]
    fetcher = config["fetcher_class"](cache=CACHE / dataset_values.dataset_name)
    stac_loader = config["stac_loader_class"]
    metadata_transformer = config["metadata_transformer_class"]()

    dataset_cache_path = CACHE / dataset_values.dataset_name
    combined_cache_path = dataset_cache_path / "combined"

    return Pipeline(
        assessor=assessor,
        fetcher=fetcher,
        extractor=ERA5NetCDFExtractor(),
        combiner=ERA5Combiner(cache=CACHE / dataset_values.dataset_name / "combined"),
        transformer=Composite(
            transformer(data_var=dataset_values.data_var).dataset_transformer,
            metadata_transformer.metadata_transformer,
            compress([dataset_values.data_var]),
        ),
        loader=stac_loader(
            time_dim="time", publisher=component.ipld_publisher("local_file", combined_cache_path / "zarr_head.cid")
        ),
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
        {script} [options] append
        {script} [options] replace
        {script} [options] interact

    Options:
        -h --help            Show this screen.
        --timespan SPAN      How much data to load along the time axis. [default: 1M]
        --daterange RANGE    The date range to load.
        --overwrite          Allow data to be overwritten.
        --pdb                Drop into debugger on error.
        --dataset DATASET    The dataset to run (e.g., "precip", "solar"). [default: precip]
    """.format(name="ETL Pipeline", script="etl_pipeline.py")
    
    arguments = docopt(usage)

    dataset_type = arguments['--dataset']
    print(f"Running pipeline for dataset type: {dataset_type}")
    mainPipeline(dataset_type)


