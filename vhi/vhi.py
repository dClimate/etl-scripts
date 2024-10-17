"""
An example using CPC Global Precipitation data.

This example uses the imperative config option, which allows you to construct a pipeline in Python code. For an example
that uses a YAML config file instead of Python code, see the CPC US Precipitation example.
"""

import pathlib

from dc_etl import filespec
from .cli import main
from .pipeline import Pipeline
# import fetch from same directory
from .fetcher import VHI
from .publisher import LocalFileIPLDPublisher
from .metadata import MetadataTransformer
from .transformer import DatasetTransformer
from .compressor import compress
from .composite import Composite
from .ipld_stac_loader import IPLDStacLoader
from .extractor import VHINetCDFExtractor
from .combiner import VHICombiner
from .assessor import VHIAssessor

HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"


def mainPipeline():
    """Run the example.
    Other than how they're configured, the two examples aren't particularly different, so we use the same command
    line interface code for both."""
    pipeline = Pipeline(
        assessor=VHIAssessor(dataset_name="vhi", time_resolution="weekly"),
        fetcher=VHI(skip_pre_parse_nan_check=True, cache=CACHE),
        extractor=VHINetCDFExtractor(),
        combiner=VHICombiner(),
        transformer=Composite(
            DatasetTransformer().dataset_transformer,
            MetadataTransformer("VHI", "vhi").metadata_transformer,
            compress(["VHI"]),
        ),
        loader=IPLDStacLoader(
            time_dim="time", publisher=LocalFileIPLDPublisher("local_file", CACHE / "vhi" / "combined" / "zarr_head.cid"), cache_location=CACHE / "vhi"
        ),
    )


    main(pipeline)


if __name__ == "__main__":
    mainPipeline()