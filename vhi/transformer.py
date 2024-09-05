from dc_etl.transform import Transformer
import xarray
from .metadata import static_metadata
import pandas as pd
import numpy as np
import dask.array as da
from dask import delayed
import datetime
from utils.helper_functions import numpydate_to_py

def set_dataset_metadata(variable: str, dataset_name: str) -> Transformer:
    """Transformer to update the zarr metadata.

    Returns
    -------
    Transformer :
        The transformer.
    """

    data_var = "VHI"

    MIN_LAT = -55.152

    MAX_LAT = 75.024

    MIN_LON = -180

    MAX_LON = 180

    time_dim="time"

    RESOLUTION = 360 / 10000  # this says there are 10000 points in 360 latitude/longitude

    identical_dimensions = ["HEIGHT", "WIDTH"]
    """
    List of dimension(s) whose values are identical in all input datasets.
    This saves Kerchunk time by having it read these dimensions only one time, from the first input file
    """

    concat_dimensions = ["time", "sat"]
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

    def preprocess_zarr(dataset: xarray.Dataset) -> xarray.Dataset:
        """
        Drops the unused data variables and converts raw coordinate text values
         to machine readable and human friendly ones.
        Specifically, time is converted to datetime.datetimes and HEIGHT/WIDTH to Lat/Lon values.
        Populates missing weeks of data with empty arrays
        Deals with duplicate values resulting from multiple sensor inputs

        Parameters
        ----------
        dataset: xr.Dataset
            The dataset to manipulate.
            This is automatically supplied when this function is submitted under xr.open_dataset()

        Returns
        -------
        dataset: xr.Dataset
            The preprocessed dataset.
        """
        unwanted_vars = [
            var for var in dataset.data_vars if var in ["VCI", "TCI", "QA", "PLATE_CARREE", "latitude", "longitude"]
        ]
        dataset = dataset.drop_vars(unwanted_vars)
        dti = pd.to_datetime(dataset.time.values, format="%Y%m%d%H%M%S%f")
        dhi = [MAX_LAT - (val + 0.5) * RESOLUTION for val in dataset.HEIGHT.values]
        dwi = [MIN_LON + (val + 0.5) * RESOLUTION for val in dataset.WIDTH.values]
        dataset = dataset.rename_dims({"HEIGHT": "latitude", "WIDTH": "longitude"})
        dataset = dataset.assign_coords({"time": dti, "latitude": dhi, "longitude": dwi})
        return dataset


    def vhi_daterange(start: str, end: str) -> tuple[datetime.datetime, datetime.datetime]:
        """
        VHI updates on a slightly odd schedule that shifts around every year.
        To ensure we request the precise days we want, we have to do some gymnastics.

        Parameters
        ----------
        start : str (isoformatted)
            First date to include
        end : str (isformatted)
            Last date to include

        Returns
        -------
        date_range : tuple[datetime.datetime, datetime.datetime]
            A range of dates formatted to match VHI's update schedule
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
        date_range = pd.DatetimeIndex([])
        for year in range(start.year, end.year + 1):
            year_end = datetime.datetime(year, 12, 29)
            if year_end > end:
                year_end = end
            year_start = datetime.datetime(year, 1, 1)
            if year_start < start:
                # we want the first valid day after the start
                # this returns the first valid day in the range, or None
                year_start = next(
                    (day for day in pd.date_range(datetime.datetime(year, 1, 1), year_end, freq="7D") if day >= start),
                    None,
                )
            # There's never a file for the last day (or two days in a leap year)
            # don't make a entry for it
            date_range = date_range.union(pd.date_range(year_start, year_end, freq="7D"))
        return date_range

    def vhi_weeks_per_year(date_range: tuple[datetime.datetime, datetime.datetime]) -> list[datetime.datetime]:
        """
        Calculate the weeks covered by VHI in a given time range, according to the logic of its update schedule

        Parameters
        ----------
        date_range : tuple[datetime.datetime, datetime.datetime]
            A range of dates formatted to match VHI's update schedule

        Returns
        -------
        vhi_weeks : list[datetime.datetime]
            A list of weeks to request expressed as datetimes
        """
        vhi_weeks = []
        for year in range(date_range[0].year, date_range[1].year + 1):
            if datetime.datetime(year, 1, 1) > date_range[0]:
                if datetime.datetime(year, 12, 31) > date_range[1]:
                    vhi_weeks.append(
                        vhi_daterange(
                            datetime.datetime(year, 1, 1).isoformat(),
                            date_range[1].isoformat(),
                        )
                    )
                else:
                    vhi_weeks.append(
                        vhi_daterange(
                            datetime.datetime(year, 1, 1).isoformat(),
                            datetime.datetime(year, 12, 31).isoformat(),
                        )
                    )
            else:
                if datetime.datetime(year, 12, 31) > date_range[1]:
                    vhi_weeks.append(vhi_daterange(date_range[0].isoformat(), date_range[1].isoformat()))
                else:
                    vhi_weeks.append(
                        vhi_daterange(
                            date_range[0].isoformat(),
                            datetime.datetime(year, 12, 31).isoformat(),
                        )
                    )
        return vhi_weeks

    def data_mean_per_time_period(dataset: xarray.DataArray) -> da.Array:
        """
        Prepare a DataArray of the mean values of multiple data arrays per time period.
        Take the nanmean since VHI satellites overlap unpredictably and there are many NaN values for each time period.
        Write the resulting DataArray to disk for further use, since preparing the mean dataest in memory causes crashes

        Parameters
        ----------
        dataset : xr.DataArray
            The DataArray in the initial Dataset holding all satellite's data for all time steps
        """
        # Take nanmean of overlapping values from multiple satellites
        # and return a DataArray without a satellite dimension
        delayed_means = [delayed(np.nanmean)(all_sats_arr, axis=0) for all_sats_arr in dataset[data_var]]
        delayed_list = [da.from_delayed(mean_arr, shape=(3616, 10000), dtype=np.float32) for mean_arr in delayed_means]
        mean_stack = da.stack(delayed_list, axis=0)  # 0 is the data axis
        print("Raw mean data calculated and held in memory")
        return mean_stack

    def create_missing_weeks(missing_weeks: list[pd.Timestamp], dataset: xarray.Dataset) -> xarray.Dataset:
        """
        Create a dataset of empty data for missing weeks, concatenate it to the input dataset, and order by time

        Parameters
        ----------
        missing_weeks : list[pd.Timestamp]
            list of weeks missing from the input dataset
        dataset : xr.Dataset
            The input dataset, missing data for any number of weeks

        Returns
        -------
        dataset : xr.Dataset
            A reindexed dataset which includes data for the calculated missing weeks
        """
        print(f"Generating empty data for {len(missing_weeks)} missing weeks of data")
        weekly_dts = pd.to_datetime(missing_weeks)

        comb_time = np.concatenate([dataset.time.values, weekly_dts])
        comb_time.sort()

        return dataset.reindex(time=comb_time)

    def get_date_range_from_dataset(dataset: xarray.Dataset) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Return the start and end date in a dataset's "time" dimension

        Parameters
        ----------
        dataset : xr.Dataset
            The xr.Dataset to be evaluated

        Returns
        -------
        tuple[datetime.datetime, datetime.datetime]
            A tuple defining the start and end date of a file's time dimension

        """
        # if not hasattr(self, "time_dim"):
        #     self.set_key_dims()
        # if there is only one date, set start and end to the same value
        if dataset[time_dim].size == 1:
            values = dataset[time_dim].values
            assert len(values) == 1
            start = end = numpydate_to_py(values[0])
        else:
            start = numpydate_to_py(dataset[time_dim][0].values)
            end = numpydate_to_py(dataset[time_dim][-1].values)
        return start, end


    def populate_missing_weeks(dataset: xarray.Dataset) -> xarray.Dataset:
        """
        Find weeks in the dataset where input VHI data is missing.
        The weeks considered follow VHI's update schedule.

        Parameters
        ----------
        dataset: xr.Dataset
            The nanmean'd and rechunked dataset

        Returns
        ----------
        dataset: xr.Dataset
            The dataset reindexed to include missing weeks of data

        """
        dataset_date_range = get_date_range_from_dataset(dataset=dataset)
        vhi_weeks = vhi_weeks_per_year(date_range=dataset_date_range)
        # filter out any weeks that are already in the dataset
        missing_weeks = [
            week_start for year_of_weeks in vhi_weeks for week_start in year_of_weeks if week_start not in dataset.time
        ]
        # populate an empty array for each missing week and concatenate it into the dataset
        if missing_weeks:
            dataset = create_missing_weeks(missing_weeks=missing_weeks, dataset=dataset)
        return dataset

    def postprocess_zarr(dataset: xarray.Dataset) -> xarray.Dataset:
        """
        Further process the dataset Zarr after it's been created in memory.
        Useful for accessing methods that can't be access within a callback method (preprocess_zarr)

        For VHI, first calculate the data for each time period as the nanmean of N satellites' worth of data
        Rechunk the resulting dataset to a size that will adequately fit
        into memory for the final write to IPLD. This happens in several stages.
        Then, populate empty arrays for missing weeks of data into the final dataset

        Parameters
        ----------
        dataset: xr.Dataset
            The dataset to manipulate.
            This is automatically supplied when this function is submitted under xr.open_dataset()

        Returns
        -------
        dataset: xr.Dataset
            The postprocessed dataset.
        """
        # Drop unneeded data variables, convert HEIGHT/WIDTH to Lat/Lon, and correctly format time as datetime.datetimes
        dataset = preprocess_zarr(dataset)
        # Take the mean of all satellites' values
        print(
            f"Taking the mean of data points from {len(dataset.sat.values)}\
                satellites for each time period (excluding nans)"
        )
        mean_data = data_mean_per_time_period(dataset=dataset)
        # lazily insert rechunked DataArray into a Dataset w/ the same coords and attrs as the original VHI
        merged_dataset = xarray.Dataset(
            data_vars=dict(VHI=(["time", "latitude", "longitude"], mean_data)),
            coords={
                "latitude": dataset.latitude.values,
                "longitude": dataset.longitude.values,
                "time": dataset.time.values,
            },
            attrs=dataset.attrs,
        )
        merged_dataset = merged_dataset.sortby(["latitude", "longitude"])
        # return the dataset w/ empty NaN arrays inserted for missing time periods.
        return populate_missing_weeks(dataset=merged_dataset)


    # Delete problematic or extraneous holdover attributes from the input files
    # Because each Copernicus Marine dataset names fields differently
    # ('latitude' vs 'lat') this list is long and duplicative
    def set_dataset_metadata(dataset: xarray.Dataset) -> xarray.Dataset:

        dataset = postprocess_zarr(dataset=dataset)
        keys_to_remove = [
            "scale_factor",
            "add_offset",
            "cdm_data_type",
            "ANCILLARY_FILES",
            "CITATION_TO_DOCUMENTS",
            "CONFIGURE_FILE_CONTENT",
            "CONTACT",
            "DAYS_PER_PERIOD",
            "FILENAME",
            "INPUT_FILENAMES",
            "INPUT_FILES",
            "INSTRUMENT",
            "DATE_BEGIN",
            "DATE_END",
            "DAYS PER PERIOD",
            "Metadata_Conventions",
            "PERIOD_OF_YEAR",
            "PRODUCT_NAME",
            "PROJECTION",
            "SATELLITE",
            "START_LATITUDE_RANGE",
            "START_LONGITUDE_RANGE",
            "END_LATITUDE_RANGE",
            "END_LONGITUDE_RANGE",
            "TIME_BEGIN",
            "TIME_END",
            "VERSION",
            "YEAR",
            "time_coverage_start",
            "time_coverage_end",
            "satellite_name",
            "standard_name_vocabulary",
            "version",
            "created",
            "creator_name",
            "creator_email",
            "creator_url",
            "date_created",
            "geospatial_lat_units",
            "geospatial_lon_units",
            # "geospatial_lat_min",
            # "geospatial_lon_min",
            # "geospatial_lat_max",
            # "geospatial_lon_max",
            "geospatial_lat_resolution",
            "geospatial_lon_resolution",
            "history",
            "id",
            "institution",
            "instrument_name",
            "naming_authority",
            "process",
            "processing_level",
            "project",
            "publisher_name",
            "publisher_email",
            "publisher_url",
            "references",
            "source",
            "summary",
        ]

        all_keys = (
            list(dataset.attrs.keys())
            + list(dataset[variable].attrs.keys())
            + list(dataset[variable].encoding.keys())
        )

        for key in all_keys:
            if key in keys_to_remove:
                dataset.attrs.pop(key, None)
                dataset["latitude"].attrs.pop(key, None)
                dataset["longitude"].attrs.pop(key, None)
                dataset[variable].attrs.pop(key, None)
                dataset[variable].encoding.pop(key, None)

        dataset.attrs.update(static_metadata())
        return dataset

    return set_dataset_metadata
