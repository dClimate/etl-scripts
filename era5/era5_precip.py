"""
An example using CPC Global Precipitation data.

This example uses the imperative config option, which allows you to construct a pipeline in Python code. For an example
that uses a YAML config file instead of Python code, see the CPC US Precipitation example.
"""

import pathlib

from dc_etl import filespec
from .cli import main

# import fetch from same directory
from .fetchers import ERA5Precip
from .pipeline import Pipeline
from dc_etl import component
from .metadata import ERA5PrecipValuesMetadataTransformer
from .transformer import ERA5FamilyDatasetTransformer

from .ipld_stac_loader import ERA5PrecipStacLoader
from .extractor import ERA5NetCDFExtractor
from .combiner import ERA5Combiner
from .assessor import ERA5PrecipValuesAssessor
from .composite import Composite
from .compressor import compress
from .base_values import ERA5PrecipValues

HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"


def mainPipeline():
    """Run the example.
    Other than how they're configured, the two examples aren't particularly different, so we use the same command
    line interface code for both."""
    dataset_values = ERA5PrecipValues()
    dataset_cache_path = CACHE / dataset_values.dataset_name
    combined_cache_path = dataset_cache_path / "combined"
    pipeline = Pipeline(
        assessor=ERA5PrecipValuesAssessor(),
        fetcher=ERA5Precip(cache=CACHE / dataset_values.dataset_name),
        extractor=ERA5NetCDFExtractor(),
        combiner=ERA5Combiner(cache=CACHE / dataset_values.dataset_name / "combined"),
        transformer=Composite(
            ERA5FamilyDatasetTransformer(data_var=dataset_values.data_var).dataset_transformer,
            ERA5PrecipValuesMetadataTransformer().metadata_transformer,
            compress([dataset_values.data_var]),
        ),
        loader=ERA5PrecipStacLoader(
            time_dim="time", publisher=component.ipld_publisher("local_file", combined_cache_path / "zarr_head.cid")
        ),
    )
    main(pipeline)


if __name__ == "__main__":
    mainPipeline()