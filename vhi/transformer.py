# from dc_etl.transform import Transformer
import xarray
import pandas as pd
import numpy as np
import dask.array as da
from dask import delayed
import datetime
from utils.helper_functions import numpydate_to_py

class DatasetTransformer():
    """Transformer to update the zarr metadata.

    Returns
    -------
    Transformer :
        The transformer.
    """

    data_var = "VHI"

    # THIS IS DIFFERENT THAN THE BBOX BECAUSE BBOX IS JUST THE UPPER AND LOWER MOST VALUES. WHEN YOU APPLY THE RESOLUTION YOU GET THESE LIMITS
    MIN_LAT = -55.152
    MAX_LAT = 75.024
    MIN_LON = -180
    MAX_LON = 180
    
    time_dim="time"

    RESOLUTION = 360 / 10000  # this says there are 10000 points in 360 latitude/longitude

    def preprocess_zarr(self, dataset: xarray.Dataset) -> xarray.Dataset:
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
        dhi = [self.MAX_LAT - (val + 0.5) * self.RESOLUTION for val in dataset.HEIGHT.values]
        # Truncate anything after 3 decimal places
        dhi = [round(val, 3) for val in dhi]
        dwi = [self.MIN_LON + (val + 0.5) * self.RESOLUTION for val in dataset.WIDTH.values]
        # Truncate anything after 3 decimal places
        dwi = [round(val, 3) for val in dwi]
        dataset = dataset.rename_dims({"HEIGHT": "latitude", "WIDTH": "longitude"})
        dataset = dataset.assign_coords({"time": dti, "latitude": dhi, "longitude": dwi})
        return dataset


    def vhi_daterange(self, start: str, end: str) -> tuple[datetime.datetime, datetime.datetime]:
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
        start, end = pd.to_datetime(start), pd.to_datetime(end)
        date_range = pd.DatetimeIndex([])

        for year in range(start.year, end.year + 1):
            # Define year start and end, adjusting for the provided start and end
            year_start = max(start, pd.Timestamp(f"{year}-01-01"))
            year_end = min(end, pd.Timestamp(f"{year}-12-29"))
            
            # Create the weekly frequency range
            weekly_dates = pd.date_range(year_start, year_end, freq="7D")
            date_range = date_range.union(weekly_dates)

        return date_range

    def vhi_weeks_per_year(self, date_range: tuple[datetime.datetime, datetime.datetime]) -> list[datetime.datetime]:
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
        start_date, end_date = date_range
        vhi_weeks = []

        for year in range(start_date.year, end_date.year + 1):
            year_start = max(datetime.datetime(year, 1, 1), start_date)
            year_end = min(datetime.datetime(year, 12, 31), end_date)

            vhi_weeks.append(self.vhi_daterange(year_start.isoformat(), year_end.isoformat()))

        return vhi_weeks

    def data_mean_per_time_period(self, dataset: xarray.DataArray) -> da.Array:
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
        delayed_means = [delayed(np.nanmean)(all_sats_arr, axis=0) for all_sats_arr in dataset[self.data_var]]
        delayed_list = [da.from_delayed(mean_arr, shape=(3616, 10000), dtype=np.float32) for mean_arr in delayed_means]
        mean_stack = da.stack(delayed_list, axis=0)  # 0 is the data axis
        print("Raw mean data calculated and held in memory")
        return mean_stack

    def create_missing_weeks(self, missing_weeks: list[pd.Timestamp], dataset: xarray.Dataset) -> xarray.Dataset:
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

    def get_date_range_from_dataset(self, dataset: xarray.Dataset) -> tuple[datetime.datetime, datetime.datetime]:
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
        if dataset[self.time_dim].size == 1:
            values = dataset[self.time_dim].values
            assert len(values) == 1
            start = end = numpydate_to_py(values[0])
        else:
            start = numpydate_to_py(dataset[self.time_dim][0].values)
            end = numpydate_to_py(dataset[self.time_dim][-1].values)
        return start, end


    def populate_missing_weeks(self, dataset: xarray.Dataset) -> xarray.Dataset:
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
        dataset_date_range = self.get_date_range_from_dataset(dataset=dataset)
        vhi_weeks = self.vhi_weeks_per_year(date_range=dataset_date_range)
        # filter out any weeks that are already in the dataset
        missing_weeks = [
            week_start for year_of_weeks in vhi_weeks for week_start in year_of_weeks if week_start not in dataset.time
        ]
        # populate an empty array for each missing week and concatenate it into the dataset
        if missing_weeks:
            dataset = self.create_missing_weeks(missing_weeks=missing_weeks, dataset=dataset)
        return dataset

    def postprocess_zarr(self, dataset: xarray.Dataset) -> xarray.Dataset:
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
        dataset = self.preprocess_zarr(dataset)
        # Take the mean of all satellites' values
        print(
            f"Taking the mean of data points from {len(dataset.sat.values)}\
                satellites for each time period (excluding nans)"
        )
        mean_data = self.data_mean_per_time_period(dataset=dataset)
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
        return self.populate_missing_weeks(dataset=merged_dataset)

    def dataset_transformer(self, dataset: xarray.Dataset, metadata_info: dict) -> xarray.Dataset:
        return self.postprocess_zarr(dataset=dataset), metadata_info
