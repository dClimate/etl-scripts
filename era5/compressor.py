import numcodecs
import xarray

from dc_etl.transform import Transformer

def compress(variables: list[str]) -> Transformer:
    """Transformer to add Blosc compression to specific variables, usually the data variable.

    Parameters
    ----------
    *variables: list[str]
        Names of the variables in the dataset to apply compression to.

    Returns
    -------
    Transformer :
        The transformer.
    """

    def compress(dataset: xarray.Dataset, metadata_info: dict) -> tuple[xarray.Dataset, dict]:
        for var in variables:
            dataset[var].encoding["compressor"] = numcodecs.Blosc()

        return dataset, metadata_info

    return compress