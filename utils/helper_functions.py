
import datetime
import pandas as pd

import numpy as np
import xarray as xr

import random
from typing import Any



def numpydate_to_py(numpy_date: np.datetime64) -> datetime.datetime:
    """
    Convert a numpy datetime object to a python standard library datetime object

    Parameters
    ----------
    np.datetime64
        A numpy.datetime64 object to be converted

    Returns
    -------
    datetime.datetime
        A datetime.datetime object

    """
    return pd.Timestamp(numpy_date).to_pydatetime()

def _is_infish(n):
    """Tell if a value is infinite-ish or not.

    An infinite-ish value may either be one of the floating point `inf` values or have an absolute value greater than
    1e100.
    """
    return np.isinf(n) or abs(n) > 1e100

def get_random_coords(dataset: xr.Dataset) -> dict[str, Any]:
    """
    Derive a dictionary of random coordinates, one for each dimension in the input dataset

    Parameters
    ----------
    dataset
        An Xarray dataset

    Returns
    -------
        A dict of {str: Any} pairing each dimension to a randomly selected coordinate value
    """
    coords_dict = {}
    # We select from dims, not coords because inserts drop all non-time_dim coords
    for dim in dataset.dims:
        coords_dict.update({dim: random.choice(dataset[dim].values)})
    return coords_dict

def check_written_value(
    data_var: str,
    orig_ds: xr.Dataset,
    prod_ds: xr.Dataset,
    threshold: float = 10e-5,
    n_checks: int = 100,
):
    """
    Check random values in the original files against the written values
    in the updated dataset at the same location for multiple samples.

    Parameters
    ----------
    data_var : str
        The name of the variable to check.
    orig_ds : xr.Dataset
        A randomly selected original dataset.
    prod_ds : xr.Dataset
        The production dataset, filtered down to the time range of the latest update.
    threshold : float, optional
        The tolerance for differences between original and parsed values.
    n_checks : int, optional
        The number of random values to check per day. Default is 100.

    Returns
    -------
    bool
        True if all checks pass, otherwise raises a ValueError.

    Raises
    ------
    ValueError
        Indicates a mismatch between source data values and values written to production.
    """

    # Number of checks will depend on the day difference of the dataset. For every day, check 100 random points.

    # Get day of the first and last time step
    first_date = numpydate_to_py(orig_ds.time.values[0])
    last_date = numpydate_to_py(orig_ds.time.values[-1])

    # Get the number of days between the first and last day
    # Calculate the absolute day difference
    day_difference = abs((last_date - first_date).days)
    # Get the number of checks
    n_checks = day_difference * n_checks

    print(f"Checking {n_checks} random values for {data_var} in the dataset.", day_difference)


    for _ in range(n_checks):
        selection_coords = get_random_coords(orig_ds)
        
        orig_val = orig_ds[data_var].sel(**selection_coords).values
        prod_val = prod_ds[data_var].sel(**selection_coords, method="nearest", tolerance=0.0001).values

        if _is_infish(orig_val) and _is_infish(prod_val):
            continue  # Both are infinity, skip to the next check
        elif np.isnan(orig_val) and np.isnan(prod_val):
            continue  # Both are NaN, skip to the next check
        elif abs(orig_val - prod_val) <= threshold:
            continue  # Values are within threshold, skip to the next check
        else:
            # Raise an error if a mismatch is found
            raise ValueError(
                "Mismatch in written values: "
                f"orig_val {orig_val} and prod_val {prod_val}."
                f"\nQuery parameters: {selection_coords}"
            )
    
    return True  # All checks passed

def check_dataset_alignment(
    self,
    new_ds: xr.Dataset,
    prod_ds: xr.Dataset
):
    """Check if the dataset aligns with the current dataset.
    We check the following:
    - The dataset has the same dimensions as the current dataset
    - The dataset has the same coordinates as the current dataset
    - The dataset has the same variables as the current dataset
    - The dataset has the same attributes as the current dataset

    This method should raise an exception if the dataset does not align.
    This prevents the user from accidentally appending or replacing data that does not align with the existing dataset.
    
    """
    # Get original dataset
    if prod_ds is None:
        return
    # Check if the dataset has the same dimensions as the current dataset except for the time dimension
    for dim in new_ds.dims:
        if dim != self.time_dim:
            if dim not in prod_ds.dims:
                raise ValueError(f"Dimension {dim} not found in the original dataset.")
    # Check if the dataset has the same coordinates as the current dataset
    for coord in new_ds.coords:
        if coord not in prod_ds.coords:
            raise ValueError(f"Coordinate {coord} not found in the original dataset.")
    # Check if the dataset has the same variables as the current dataset
    for var in new_ds.data_vars:
        if var not in prod_ds.data_vars:
            raise ValueError(f"Variable {var} not found in the original dataset.")
    # Check if the dataset has the same attributes as the current dataset
    for attr in new_ds.attrs:
        if attr not in prod_ds.attrs:
            raise ValueError(f"Attribute {attr} not found in the original dataset.")
    # Check the dimensions sizes for the latitude and longitude align
    if new_ds.latitude.size != prod_ds.latitude.size:
        raise ValueError("Latitude dimensions do not match.")
    if new_ds.longitude.size != prod_ds.longitude.size:
        raise ValueError("Longitude dimensions do not match.")
    # Check the bounds for the latitude and longitude dimensions
    if new_ds.latitude.values[0] != prod_ds.latitude.values[0]:
        raise ValueError("Latitude bounds do not match.")
    if new_ds.longitude.values[0] != prod_ds.longitude.values[0]:
        raise ValueError("Longitude bounds do not match.")
    # Check the resolution for the latitude and longitude dimensions
    if new_ds.latitude.values[1] - new_ds.latitude.values[0] != prod_ds.latitude.values[1] - prod_ds.latitude.values[0]:
        raise ValueError("Latitude resolution does not match.")
    if new_ds.longitude.values[1] - new_ds.longitude.values[0] != prod_ds.longitude.values[1] - prod_ds.longitude.values[0]:
        raise ValueError("Longitude resolution does not match.")
    # Check the time resolution
    if new_ds.time.values[1] - new_ds.time.values[0] != prod_ds.time.values[1] - prod_ds.time.values[0]:
        raise ValueError("Time resolution does not match.")
    self.info("Dataset alignment check passed.")