from abc import ABC

import numpy as np
import typing


class Attributes(ABC):
    """
    Abstract base class containing default attributes of Zarr ETLs
    These can be overriden in the ETL managers for a given ETL as needed
    """

    organization: str = "dClimate"
    """
    Name of the organization (your organization) hosting the data being published. Used in STAC metadata.
    """

    dataset_name = ""
    """
    The name of each ETL is built recursively by appending each child class name to the inherited name
    """

    collection_name = ""
    """
    Name of the collection
    """

    file_type = None
    """
    The file type of each child class (and edition if relevant), e.g. GRIB1 for ERA5 data, GRIB2 for RTMA, or NetCDF
    for Copernicus Marine Service

    Used to trigger file format-appropriate functions and methods for Kerchunking and Xarray operations.
    """

    protocol: typing.Literal["s3", "file"] = "file"
    """
    Remote protocol string for MultiZarrToZarr and Xarray to use when opening input files. 'file' for local, 's3'
    for S3, etc. See fsspec docs for more details.
    """

    identical_dimensions = []
    """
    List of dimension(s) whose values are identical in all input datasets. This saves Kerchunk time by having it
    read these dimensions only one time, from the first input file
    """

    concat_dimensions = ["time"]
    """
    List of dimension(s) by which to concatenate input files' data variable(s) -- usually time, possibly with some
    other relevant dimension
    """

    data_var_dtype: float = "<f4"
    """
    The data type of the data variable
    """

    spatial_resolution: float | None = None
    """
    The spatial resolution of a dataset in decimal degrees
    """

    spatial_precision: float | None = None
    """
    The spatial resolution of a dataset in decimal degrees
    """

    time_resolution: str = ""
    """
    The time resolution of the dataset as a string (e.g. "hourly", "daily", "monthly", etc.)
    """

    update_attributes: list[str] = ["date_range", "update_previous_end_date"]
    """
    Certain fields of a dataset should not be overwritten until after a parse completes to avoid confusion
     if a parse fails midway.
    """

    update_cadence: str | None = None
    """
    The frequency with which a dataset is updated.
    """

    missing_value: str = ""
    """
    Indicator of a missing value in a dataset
    """

    tags: list[str] = [""]
    """
    Tags for dataset.
    """

    dataset_category: typing.Literal["observation", "forecast", "ensemble", "hindcast"] = "observation"
    """
    The type of climate data provided in a given dataset. Used to control various processes.
    Valid options include "observation", "forecast", "ensemble", and "hindcast".

    Defaults to "observation".

    Ensembles and hindcasts are necessarily forecasts and semantically should be understood
    to provide (more elaborated) forecast data with 5 and 6 dimensions. Accordingly, "forecast"
    should be understood to specify 4 dimensional forecast data w/out ensembles or hindcasts.
    """

    forecast_hours: list[int] = []
    """"
    Hours provided by the forecast, if any.
    """

    ensemble_numbers: list[int] = []
    """
    Numbers uses for ensemble, if any.
    """

    hindcast_steps: list[int] = []
    """
    Steps used for hindcast, if any.
    """

    update_cadence_bounds: tuple[np.timedelta64, np.timedelta64] | None = None
    """
    If a dataset doesn't update on a monotonic schedule return a tuple noting the lower and upper bounds of acceptable
    updates. Intended to prevent time contiguity checks from short-circuiting valid updates for datasets with
    non-monotic update schedules.
    """

    has_nans: bool = False
    """
    If True, disable quality checks for NaN values to prevent wrongful flags
    Default value set as False
    """

    open_dataset_kwargs = {}
    """Some dataset types (e.g. HDF5) need special kwargs to open in Xarray. This will pass them automatically
    during post-parse QC so these datasets can be checked automatically without issue"""

    bbox_rounding_value: int = 5
    """
    The number of decimal places to round bounding box values to.
    """

    final_lag_in_days: int = None
    """
    The number of days betweenm an observation taking place and its publication in finalized form
    w/in the dataset. For example, if today is April 6th and the latest data in a dataset is for April 1st,
    it has a 5 day lag.

    May stretch into the hundreds or thousands for some datasets that publish finalized data on a an extreme lag.
    """

    preliminary_lag_in_days: int = None
    """
    The number of days betweenm an observation taking place and its publication in preliminary form
    w/in the dataset. For example, if today is April 6th and the latest data in a dataset is for April 1st,
    it has a 5 day lag.

    Only applicable to datasets that publish preliminary data, for example CHIRPS Preliminary
    """

    expected_nan_frequency: float = 0.0
    """
    Datasets contain NaN values in varying proportions depending on how the source provider encodes data.

    Updates with unusual proportions of NaN values possibly represent possibly corrupted data
    from the source provider and should be investigated manually.

    This property encodes the anticipated proportion of NaNs in a daily dataset, based on empirical study
    of the dataaset in question
    """

    EXTREME_VALUES_BY_UNIT = {"deg_C": (-90, 60), "K": (183.15, 333.15), "deg_F": (-129, 140)}
    """
    minimum and maximum permissible values for common units
    """

