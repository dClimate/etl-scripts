
from kerchunk import combine
import abc
import time

import orjson
import xarray

import pathlib

from dc_etl import filespec


from dc_etl.filespec import FileSpec
from dc_etl.combine import Combiner

HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)
CACHE = HERE / "datasets"

class ERA5Combiner(Combiner):
    """Default combiner.

    Delegates transformation to preprocessors and postprocessors that are passed in at initialization time and are, in
    turn, passed into Kerchunk's MultiZarrToZarr implementation.

    Parameters
    ----------
    output_folder : FileSpec
        Location of folder to write combined Zarr JSON to. The filename will be generated dynamically to be unique.

    concat_dims : list[str]
        Names of the dimensions to expand with

    identical_dims : list[str]
        Variables that are to be copied across from the first input dataset, because they do not vary

    preprocessors : list[Preprocessor]
        Preprocessors to apply when combining data. Preproccessors are callables of the same kind used by
        `kerchunk.MultiZarrToZarr`. What little documentation there is of them seems to be here:
        https://fsspec.github.io/kerchunk/tutorial.html#preprocessing


    postprocessors : list[Postprocessor]
        Postprocessors to apply when combining data. Postproccessors are callables of the same kind used by
        `kerchunk.MultiZarrToZarr`. What little documentation there is of them seems to be here:
        https://fsspec.github.io/kerchunk/tutorial.html#postprocessing
    """

    identical_dimensions = ["longitude"]
    """
    List of dimension(s) whose values are identical in all input datasets.
    This saves Kerchunk time by having it read these dimensions only one time, from the first input file
    """

    concat_dimensions = ["time"]
    """
    List of dimension(s) by which to concatenate input files' data variable(s)
        -- usually time, possibly with some other relevant dimension
    """

    protocol = "file"
    """
    Remote protocol string for MultiZarrToZarr and Xarray to use when opening input files.
    'File' for local, 's3' for S3, etc.
    See fssp
    """

    def __init__(
        self,
        cache: FileSpec,
    ):
        self.output_folder = cache

    def __call__(self, sources: list[FileSpec]) -> xarray.Dataset:
        """Implementation of meth:`Combiner.__call__`.

        Calls `kerchunk.MultiZarrToZarr`.
        """

        coo_mapper = {"time": "cf:time"}
        ensemble = combine.MultiZarrToZarr(
            [source.path for source in sources],
            remote_protocol=self.protocol,
            identical_dims=self.identical_dimensions,
            coo_map=coo_mapper,
            concat_dims=self.concat_dimensions,
            # preprocess=self.preprocess_kerchunk,
            # postprocess=postprocessor(self.postprocessors),
        )

        output = self.output_folder / f"combined_zarr_{int(time.time()):d}.json"
        with output.open("wb") as f_out:
            f_out.write(orjson.dumps(ensemble.translate()))

        return xarray.open_dataset(
            "reference://",
            engine="zarr",
            backend_kwargs={
                "consolidated": False,
                "storage_options": {
                    "fo": output.path,
                    "remote_protocol": output.fs.protocol[0],  # Does this always work for protocol?
                },
            },
        )


class CombinePreprocessor(abc.ABC):
    """See: https://fsspec.github.io/kerchunk/tutorial.html#preprocessing"""

    @abc.abstractmethod
    def __call__(self, refs: dict) -> dict:
        """See: https://fsspec.github.io/kerchunk/tutorial.html#preprocessing"""


class CombinePostprocessor(abc.ABC):
    """See: https://fsspec.github.io/kerchunk/tutorial.html#postprocessing"""

    @abc.abstractmethod
    def __call__(self, refs: dict) -> dict:
        """See: https://fsspec.github.io/kerchunk/tutorial.html#postprocessing"""