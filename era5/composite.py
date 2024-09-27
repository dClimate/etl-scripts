import xarray
from dc_etl.transform import Transformer

class Composite:
    """A composition of transformers.

    The transformers used in the composition will be called in the order passed in with the output of one feeding the
    input of the next, in a pipeline fashion.

    Parameters
    ----------
    *transformers : Transformer
        The transformers to use for the composition.

    Returns
    -------
    Transformer :
        A single transformer composed of the transformers passed in as a pipeline.
    """

    @classmethod
    def _from_config(cls, config):
        return cls(*[transformer.as_component("transformer") for transformer in config["transformers"]])

    def __init__(self, *transformers: Transformer):
        self.transformers = transformers

    def __call__(self, dataset: xarray.Dataset, metadata_info: dict) -> xarray.Dataset:
        for transform in self.transformers:
            dataset, metadata = transform(dataset, metadata_info)  # Pass metadata to each transformer
        return dataset, metadata