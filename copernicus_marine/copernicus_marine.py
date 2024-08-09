"""
An example using CPC Global Precipitation data.

This example uses the imperative config option, which allows you to construct a pipeline in Python code. For an example
that uses a YAML config file instead of Python code, see the CPC US Precipitation example.
"""

import pathlib

from dc_etl import filespec
from .cli import main
# import fetch from same directory
from .fetcher import CopernicusOceanSeaSurfaceHeight
from dc_etl.pipeline import Pipeline
from dc_etl import component


HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"


def mainPipeline():
    """Run the example.
    Other than how they're configured, the two examples aren't particularly different, so we use the same command
    line interface code for both."""
    pipeline = Pipeline(
        fetcher=CopernicusOceanSeaSurfaceHeight(skip_pre_parse_nan_check=True, cache=CACHE),
        extractor=component.extractor("netcdf"),
        combiner=component.combiner(
            "default",
            output_folder=CACHE / "copernicus_ocean" / "sea_level" / "combined",
            concat_dims=["time"],
            identical_dims=["latitude", "latitude"],
            preprocessors=[component.combine_preprocessor("fix_fill_value", -9.96921e36)],
        ),
        transformer=component.transformer("compress", ["sla"]),
        loader=component.loader(
            "ipld", time_dim="time", publisher=component.ipld_publisher("local_file", CACHE / "copernicus_ocean" / "sea_level" / "combined" / "zarr_head.cid")
        ),
    )

    main(pipeline)


if __name__ == "__main__":
    mainPipeline()