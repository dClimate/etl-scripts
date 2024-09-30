import xarray
from .base_values import ERA5Family


class ERA5FamilyDatasetTransformer(ERA5Family):

    def __init__(self, data_var):
        self.data_var = data_var  # Set data_var for this instance


    def standardize_longitudes(dataset: xarray.Dataset) -> xarray.Dataset:
        """
        Convert the longitude coordinates of a dataset from 0 - 360 to -180 to 180.

        Parameters
        ----------
        xr.Dataset
            A dataset with longitudes from 0 to 360
        Returns
        -------
        xr.Dataset
            A dataset with longitudes from -180 to 180

        """
        # Convert longitudes from 0 - 360 to -180 to 180
        dataset = dataset.assign_coords(longitude=(((dataset.longitude + 180) % 360) - 180))
        # After converting, the longitudes may still start at zero. This reorders the longitude coordinates from -180
        # to 180 if necessary.
        return dataset.sortby(["latitude", "longitude"])
    @classmethod
    def postprocess_zarr(cls, dataset: xarray.Dataset, data_var: str) -> xarray.Dataset:
        """
        Perform any necessary pre-processing to the dataset on load.
        Change the lat/lon coordinate names to the dClimate standard "latitude" and "longitude".
        Change the latitude to sort in ascending order.
        Convert coordinates outside of -180 to 180 into that standard range
         using the formula at https://docs.xarray.dev/en/stable/generated/xarray.DataArray.assign_coords.html

        Parameters
        ----------
        dataset : xr.Dataset
            The dataset to manipulate.
            This is automatically supplied when this function is submitted under xr.open_dataset()
        """
        dataset["time"].encoding["_FillValue"] = cls.missing_value
        dataset["time"].encoding["missing_value"] = cls.missing_value
        dataset = dataset.rename({"lat": "latitude", "lon": "longitude"})
        dataset = dataset.sortby("latitude")
        # Some datasets come packaged with different coordinates which break parses and require renaming. if its needed it will do it
        # Otherwise it will just return the dataset
        dataset = dataset.rename_vars(
            {[key for key in dataset.data_vars][0]: data_var}
        )  # GRIB CDO process seems to remove the variable name
        return cls.standardize_longitudes(dataset)

    def dataset_transformer(self, dataset: xarray.Dataset, metadata_info: dict) -> tuple[xarray.Dataset, dict]:
        return self.postprocess_zarr(dataset=dataset, data_var=self.data_var), metadata_info

class ERA5SeaDatasetTransformer(ERA5FamilyDatasetTransformer):

    def __init__(self, data_var):
        self.data_var = data_var  # Set data_var for this instance

    @classmethod
    def postprocess_zarr(self, dataset: xarray.Dataset, data_var: str) -> xarray.Dataset:
        """
        Sea layers must be further postprocessed to fit with the Arbol's Zarr file standard.
        Sea layers come packaged with ['number','surface','step'] coordinates which will break parses,
        despite providing no relevant information when exporting a single layer.
        """
        dataset = super().postprocess_zarr(dataset=dataset, data_var=data_var)
        dataset = dataset.rename_vars(
            {[key for key in dataset.data_vars][0]: data_var}
        )  # GRIB CDO process seems to remove the variable name
        return dataset
    
    def dataset_transformer(self, dataset: xarray.Dataset, metadata_info: dict) -> tuple[xarray.Dataset, dict]:
        return self.postprocess_zarr(dataset=dataset, data_var=self.data_var), metadata_info


class ERA5VolumetricSoilWaterTransformer(ERA5FamilyDatasetTransformer):

    def __init__(self, data_var):
        self.data_var = data_var  # Set data_var for this instance

    @classmethod
    def postprocess_zarr(self, dataset: xarray.Dataset, data_var: str) -> xarray.Dataset:
        """
        Volumetric Soil Water layers must be further postprocessed to fit with the Arbol's Zarr file standard.
        VSW layers come packaged with ['depth','bnds'] dimensions
         and a 'depth_bnds' variable which will break parses,
        despite providing no relevant information when exporting a single layer.
        """
        dataset = dataset.drop_vars(["depth", "depth_bnds"])
        dataset = super().postprocess_zarr(dataset=dataset, data_var=data_var)
        # reduce the `swvl` data layer to only the relevant dimensions of time, latitude, and longitude
        dataset = dataset.squeeze()
        # restore time dim from squeeze; this is necessary for quality checks
        if "time" not in dataset.dims:
            dataset = dataset.expand_dims("time")
        return dataset

    def dataset_transformer(self, dataset: xarray.Dataset, metadata_info: dict) -> tuple[xarray.Dataset, dict]:
        return self.postprocess_zarr(dataset=dataset, data_var=self.data_var), metadata_info


