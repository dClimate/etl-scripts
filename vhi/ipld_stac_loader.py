from dc_etl.load import Loader
from dc_etl.ipld.loader import IPLDPublisher

import ipldstore
import xarray
from dc_etl.fetch import Timespan
from dataset_manager.utils.metadata import Metadata
from dataset_manager.utils.store import IPLD
from dataset_manager.utils.logging import Logging
import numpy as np
import datetime

class IPLDStacLoader(Loader, Metadata, Logging):
    """Use IPLD to store datasets."""
    # Needed for the STAC
    collection_name = "VHI"
    organization = "dClimate"
    dataset_name = "vhi"
    time_resolution = "weekly"
    use_compression = True
    unit_of_measurement = "vegetative health score"
    requested_zarr_chunks={
            "time": 200,
            "latitude": 113,
            "longitude": 250,
    }
    dataset_start_date = datetime.datetime(1981, 1, 1)
    encryption_key = None
    missing_value = -999


    @classmethod
    def _from_config(cls, config):
        config["publisher"] = config["publisher"].as_component("ipld_publisher")
        return cls(**config)

    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str = None):
        super().__init__(host="http://127.0.0.1:5001")
        Logging.__init__(self, dataset_name="vhi")
        self.time_dim = time_dim
        self.publisher = publisher
        metadata = {
            "title": "Global and Regional Vegetation Health Index (VHI)",
            "license": "Public",
            "provider_description": "The National Ocean and Atmospheric Administration's (NOAA) Center for Satellite Applications and Research (STAR) uses innovative science and applications to transform satellite observations of the earth into meaninguful information essential to society's evolving environmental, security, and economic decision-making.",
            "provider_url": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/index.php",
            "terms_of_service": "https://www.ngdc.noaa.gov/ngdcinfo/privacy.html",
            "coordinate_reference_system": "EPSG:32662",
            "organization": "NESDIS STAR",
            "publisher": "NESDIS STAR",
        }

        self.store = IPLD(self)
        self.data_var = "VHI"
        self.host = "http://127.0.0.1:5001" 
        self.requested_ipfs_chunker = "size-113000"
        self.metadata = metadata
        self.cache_location=cache_location
        self.requested_dask_chunks = {"time": 200, "latitude": 226, "longitude": -1}  # 1.8 GB

    def check_dataset_alignment(self, dataset: xarray.Dataset):
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
        original_dataset = self.dataset()
        # If no original dataset exists, we can't check alignment
        if original_dataset is None:
            return
        # Check if the dataset has the same dimensions as the current dataset except for the time dimension
        for dim in dataset.dims:
            if dim != self.time_dim:
                if dim not in original_dataset.dims:
                    raise ValueError(f"Dimension {dim} not found in the original dataset.")
        # Check if the dataset has the same coordinates as the current dataset
        for coord in dataset.coords:
            if coord not in original_dataset.coords:
                raise ValueError(f"Coordinate {coord} not found in the original dataset.")
        # Check if the dataset has the same variables as the current dataset
        for var in dataset.data_vars:
            if var not in original_dataset.data_vars:
                raise ValueError(f"Variable {var} not found in the original dataset.")
        # Check if the dataset has the same attributes as the current dataset
        for attr in dataset.attrs:
            if attr not in original_dataset.attrs:
                raise ValueError(f"Attribute {attr} not found in the original dataset.")
        # Check the dimensions sizes for the latitude and longitude align
        if dataset.latitude.size != original_dataset.latitude.size:
            raise ValueError("Latitude dimensions do not match.")
        if dataset.longitude.size != original_dataset.longitude.size:
            raise ValueError("Longitude dimensions do not match.")
        # Check the bounds for the latitude and longitude dimensions
        if dataset.latitude.values[0] != original_dataset.latitude.values[0]:
            raise ValueError("Latitude bounds do not match.")
        if dataset.longitude.values[0] != original_dataset.longitude.values[0]:
            raise ValueError("Longitude bounds do not match.")
        # Check the resolution for the latitude and longitude dimensions
        if dataset.latitude.values[1] - dataset.latitude.values[0] != original_dataset.latitude.values[1] - original_dataset.latitude.values[0]:
            raise ValueError("Latitude resolution does not match.")
        if dataset.longitude.values[1] - dataset.longitude.values[0] != original_dataset.longitude.values[1] - original_dataset.longitude.values[0]:
            raise ValueError("Longitude resolution does not match.")
        # Check the time resolution
        if dataset.time.values[1] - dataset.time.values[0] != original_dataset.time.values[1] - original_dataset.time.values[0]:
            raise ValueError("Time resolution does not match.")
        self.info("Dataset alignment check passed.")

    def static_metadata(self):
        return self.metadata

    def cleanup_files(self):
        # Check if the path exists and is a directory
        if self.cache_location.exists() and self.cache_location.fs.isdir(self.cache_location.path):
            # List all files in the directory
            file_list = self.cache_location.fs.ls(self.cache_location.path, detail=False)
            for file_path in file_list:
                # Check if the item is a file (not a directory)
                if self.cache_location.fs.isfile(file_path):
                    self.cache_location.fs.rm(file_path)
        else:
            print(f"{self.cache_location.path} does not exist or is not a directory.")



    def prepare_publish_stac_metadata(self, cid, dataset: xarray.Dataset, rebuild=False):
        """Prepare the STAC metadata for the dataset."""
        self.set_custom_latest_hash(str(cid))
        self.create_root_stac_catalog()
        self.create_stac_collection(dataset=dataset)
        self.create_stac_item(dataset=dataset)
        return dataset

    def initial(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Start writing a new dataset."""
        mapper = self._mapper()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        self.check_dataset_alignment(dataset)
        # Convert numpy.datetime64 to string YYYYMMDDHH format
        dataset.attrs["date_range"] = [
            np.datetime_as_string(span.start, unit='h').replace('-', '').replace(':', '').replace('T', ''),
            np.datetime_as_string(span.end, unit='h').replace('-', '').replace(':', '').replace('T', '')
        ]
        dataset = self.set_zarr_metadata(dataset)
        # Chunk the dataset to the requested dask chunks
        dataset = dataset.chunk(self.requested_dask_chunks)
        dataset.to_zarr(store=mapper, consolidated=True)
        cid = mapper.freeze()
        self.info("Preparing Stac Metadata")
        self.prepare_publish_stac_metadata(cid, dataset, rebuild=True)
        self.publisher.publish(cid)
        self.info(f"Published {cid}")
        self.cleanup_files()

    def append(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Append data to an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        self.check_dataset_alignment(dataset)
        # Extract the start and end times from the old and new datasets
        old_start = original_dataset[self.time_dim].values[0]
        new_end = dataset[self.time_dim].values[-1]
        # Update the date_range metadata using the start of the existing dataset and the end of the new data
        dataset.attrs["date_range"] = [
            np.datetime_as_string(old_start, unit='h').replace('-', '').replace(':', '').replace('T', ''),
            np.datetime_as_string(new_end, unit='h').replace('-', '').replace(':', '').replace('T', '')
        ]
        # Chunk the dataset to the requested dask chunks
        dataset = dataset.chunk(self.requested_dask_chunks)
        dataset.to_zarr(store=mapper, consolidated=True, append_dim=self.time_dim)
        cid = mapper.freeze()
        self.info("Preparing Stac Metadata")
        self.create_stac_item(dataset=dataset)
        self.publisher.publish(cid)
        self.info(f"Published {cid}")
        self.cleanup_files()

    def replace(self, replace_dataset: xarray.Dataset, span: Timespan | None = None):
        """Replace a contiguous span of data in an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
        self.check_dataset_alignment(replace_dataset)
        region = (
            self._time_to_integer(original_dataset, span.start),
            self._time_to_integer(original_dataset, span.end) + 1,
        )

        replace_dataset = replace_dataset.sel(**{self.time_dim: slice(*span)})
        replace_dataset = replace_dataset.drop_vars([dim for dim in replace_dataset.dims if dim != self.time_dim])
        original_dataset.attrs["bbox"] = original_dataset.attrs["bbox"]
        original_dataset.attrs["date_range"] = original_dataset.attrs["date_range"]
        replace_dataset.to_zarr(store=mapper, consolidated=True, region={self.time_dim: slice(*region)})

        cid = mapper.freeze()
        self.publisher.publish(cid)
        self.info(f"Published {cid}")
        self.cleanup_files()

    def dataset(self) -> xarray.Dataset:
        """Convenience method to get the currently published dataset."""
        mapper = self._mapper(root=self.publisher.retrieve())
        return xarray.open_zarr(store=mapper, consolidated=True)

    def _mapper(self, root=None):
        mapper = ipldstore.get_ipfs_mapper(host=self.host, chunker=self.requested_ipfs_chunker)
        if root is not None:
            mapper.set_root(root)
        return mapper

    def _time_to_integer(self, dataset, timestamp):
        # It seems like an oversight in xarray that this is the best way to do this.
        nearest = dataset.sel(**{self.time_dim: timestamp, "method": "nearest"})[self.time_dim]
        return list(dataset[self.time_dim].values).index(nearest)


# print(dataset)
# data_point = dataset.sel(
#     time="2024-01-01", 
#     latitude=44.982, 
#     longitude=44.982, 
#     # method="nearest"
# )

# # Access the VHI variable at the selected point
# vhi_value = data_point['VHI'].values
# print(vhi_value)