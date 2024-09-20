"""
An example using CPC Global Precipitation data.

This example uses the imperative config option, which allows you to construct a pipeline in Python code. For an example
that uses a YAML config file instead of Python code, see the CPC US Precipitation example.
"""

import pathlib

from dc_etl import filespec
from .cli import main
# import fetch from same directory
from .fetchers import ERA5SurfaceSolarRadiationDownwards
from dc_etl.pipeline import Pipeline
from dc_etl import component
from .metadata import ERA5SurfaceSolarRadiationDownwardsValuesMetadataTransformer
from .transformer import ERA5FamilyDatasetTransformer

from .ipld_stac_loader import ERA5SurfaceSolarRadiationDownwardsStacLoader
from .extractor import ERA5NetCDFExtractor
from .combiner import ERA5Combiner


HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"


def mainPipeline():
    """Run the example.
    Other than how they're configured, the two examples aren't particularly different, so we use the same command
    line interface code for both."""
    metadataTransformer = ERA5SurfaceSolarRadiationDownwardsValuesMetadataTransformer()
    era5Transformer = ERA5FamilyDatasetTransformer()
    era5_instance = ERA5SurfaceSolarRadiationDownwards()
    pipeline = Pipeline(
        fetcher=ERA5SurfaceSolarRadiationDownwards(cache=CACHE / era5_instance.dataset_name),
        extractor=ERA5NetCDFExtractor(),
        combiner=ERA5Combiner(cache=CACHE / era5_instance.dataset_name / "combined"),
        transformer=component.transformer(
            "composite",
            era5Transformer.dataset_transformer,
            metadataTransformer.metadata_transformer,
            component.transformer("compress", [era5_instance.data_var]),
        ),
        loader=ERA5SurfaceSolarRadiationDownwardsStacLoader(
            time_dim="time", publisher=component.ipld_publisher("local_file", CACHE / era5_instance.dataset_name / "combined" / "zarr_head.cid")
        ),
    )


    main(pipeline)


if __name__ == "__main__":
    mainPipeline()