from dc_etl.load import Loader
from dc_etl.ipld.loader import IPLDPublisher

import xarray
from dc_etl.fetch import Timespan
from dataset_manager.utils.metadata import Metadata
from dataset_manager.utils.store import IPLD
from dataset_manager.utils.logging import Logging
from py_hamt import HAMT, IPFSStore
import numpy as np
from utils.helper_functions import check_dataset_alignment, check_written_value

from .base_values import (
    CopernicusOceanSeaSurfaceHeightValues,
    CopernicusOceanTemp0p5DepthValues,
    CopernicusOceanTemp1p5DepthValues,
    CopernicusOceanTemp6p5DepthValues,
    CopernicusOceanSalinity0p5DepthValues,
    CopernicusOceanSalinity1p5DepthValues,
    CopernicusOceanSalinity2p6DepthValues,
    CopernicusOceanSalinity25DepthValues,
    CopernicusOceanSalinity109DepthValues
)

class IPLDStacLoader(Loader, Metadata, Logging):
    """Use IPLD to store datasets."""
    # Needed for the STAC
    collection_name = "Copernicus Marine"
    organization = "dClimate"
    use_compression = True
    encryption_key = None
    dataset_name =  None
    time_resolution = None

    @classmethod
    def _from_config(cls, config):
        config["publisher"] = config["publisher"].as_component("ipld_publisher")
        return cls(**config)

    def __init__(self, time_dim: str, publisher: IPLDPublisher, dataset_name: str, time_resolution: str, cache_location: str | None = None):
        super().__init__(host="http://127.0.0.1:5001")
        self.time_dim = time_dim
        self.publisher = publisher
        metadata = {
            "title": "Copernicus Marine Ocean",
            "license": "CC-BY-4.0",
            "provider_description": "Copernicus Marine",
            "provider_url": "https://marine.copernicus.eu/",
            "terms_of_service": "https://marine.copernicus.eu/services-portfolio/service-commitments/",
            "coordinate_reference_system": "EPSG:4326",
            "organization": "Copernicus Marine",
            "publisher": "Copernicus Marine",
        }
        IPLDStacLoader.dataset_name = dataset_name
        IPLDStacLoader.time_resolution = time_resolution
        self.cache_location = cache_location
        self.store = IPLD(self)
        self.host = "http://127.0.0.1:5001" 
        self.metadata = metadata
        
    def static_metadata(self):
        return self.metadata

    def prepare_publish_stac_metadata(self, cid, dataset: xarray.Dataset, rebuild=False):
        """Prepare the STAC metadata for the dataset."""
        self.set_custom_latest_hash(str(cid))
        self.create_root_stac_catalog()
        self.create_stac_collection(dataset=dataset, rebuild=rebuild)
        self.create_stac_item(dataset=dataset)
        return dataset

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

    def initial(self, dataset: xarray.Dataset, span: Timespan | None = None, **kwargs):
        """Start writing a new dataset."""

        mapper = self._mapper()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})

        # Convert numpy.datetime64 to string YYYYMMDDHH format
        dataset.attrs["date_range"] = [
            np.datetime_as_string(span.start, unit='h').replace('-', '').replace(':', '').replace('T', ''),
            np.datetime_as_string(span.end, unit='h').replace('-', '').replace(':', '').replace('T', '')
        ]
        dataset.attrs["bbox"] = self.bbox
        dataset = self.set_zarr_metadata(dataset, overwrite=True)
        dataset = dataset.chunk(self.requested_dask_chunks)
        dataset.to_zarr(store=mapper, consolidated=True)
        cid = mapper.root_node_id
        self.info("Preparing Stac Metadata")
        self.prepare_publish_stac_metadata(cid, dataset, rebuild=True)
        self.publisher.publish(cid)
        self.info(f"Published {cid}")
        new_dataset = self.dataset()
        check_written_value(data_var=self.data_var, orig_ds=dataset, prod_ds=new_dataset)
        self.cleanup_files()

    def append(self, dataset: xarray.Dataset, span: Timespan | None = None, **kwargs):
        """Append data to an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        check_dataset_alignment(self, new_ds=dataset, prod_ds=original_dataset)
        # Extract the start and end times from the old and new datasets
        old_start = original_dataset[self.time_dim].values[0]
        new_end = dataset[self.time_dim].values[-1]
        # Update the date_range metadata using the start of the existing dataset and the end of the new data
        dataset.attrs["date_range"] = [
            np.datetime_as_string(old_start, unit='h').replace('-', '').replace(':', '').replace('T', ''),
            np.datetime_as_string(new_end, unit='h').replace('-', '').replace(':', '').replace('T', '')
        ]
        dataset.attrs["bbox"] = self.bbox
        dataset = dataset.chunk(self.requested_dask_chunks)
        dataset.to_zarr(store=mapper, consolidated=True, append_dim=self.time_dim)
        cid = mapper.root_node_id
        self.info("Preparing Stac Metadata")
        self.create_stac_item(dataset=dataset)
        self.publisher.publish(cid)
        self.info(f"Published {cid}")
        # Get the new dataset
        new_dataset = self.dataset()
        # Check the written values
        check_written_value(data_var=self.data_var, orig_ds=dataset, prod_ds=new_dataset)
        self.cleanup_files()

    def replace(self, replace_dataset: xarray.Dataset, span: Timespan | None = None, **kwargs):
        # Print
        """Replace a contiguous span of data in an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
        check_dataset_alignment(self, new_ds=replace_dataset, prod_ds=original_dataset)
        region = (
            self._time_to_integer(original_dataset, span.start),
            self._time_to_integer(original_dataset, span.end) + 1,
        )

        replace_dataset = replace_dataset.sel(**{self.time_dim: slice(*span)})
        replace_dataset = replace_dataset.drop_vars([dim for dim in replace_dataset.dims if dim != self.time_dim])
        original_dataset.attrs["bbox"] = original_dataset.attrs["bbox"]
        original_dataset.attrs["date_range"] = original_dataset.attrs["date_range"]
        replace_dataset.to_zarr(store=mapper, consolidated=True, region={self.time_dim: slice(*region)})

        cid = mapper.root_node_id
        self.publisher.publish(cid)
        self.info(f"Published {cid}")
        new_dataset = self.dataset()
        check_written_value(data_var=self.data_var, orig_ds=replace_dataset, prod_ds=new_dataset)
        self.cleanup_files()

    def dataset(self) -> xarray.Dataset:
        """Convenience method to get the currently published dataset."""
        mapper = self._mapper(root=self.publisher.retrieve())
        return xarray.open_zarr(store=mapper, consolidated=True)

    def _mapper(self, root=None):
        if root is None:
            mapper = HAMT(store=IPFSStore())
        else:
            mapper = HAMT(store=IPFSStore(), root_node_id=root)
        return mapper

    def _time_to_integer(self, dataset, timestamp):
        # It seems like an oversight in xarray that this is the best way to do this.
        nearest = dataset.sel(**{self.time_dim: timestamp, "method": "nearest"})[self.time_dim]
        return list(dataset[self.time_dim].values).index(nearest)

class CopernicusOceanSeaSurfaceHeightLoader(IPLDStacLoader, CopernicusOceanSeaSurfaceHeightValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanSeaSurfaceHeightValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanTemp0p5DepthLoader(IPLDStacLoader, CopernicusOceanTemp0p5DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanTemp0p5DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanTemp1p5DepthLoader(IPLDStacLoader, CopernicusOceanTemp1p5DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanTemp1p5DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanTemp6p5DepthLoader(IPLDStacLoader, CopernicusOceanTemp6p5DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanTemp6p5DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanSalinity0p5DepthLoader(IPLDStacLoader, CopernicusOceanSalinity0p5DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanSalinity0p5DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanSalinity1p5DepthLoader(IPLDStacLoader, CopernicusOceanSalinity1p5DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanSalinity1p5DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanSalinity2p6DepthLoader(IPLDStacLoader, CopernicusOceanSalinity2p6DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanSalinity2p6DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanSalinity25DepthLoader(IPLDStacLoader, CopernicusOceanSalinity25DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanSalinity25DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)

class CopernicusOceanSalinity109DepthLoader(IPLDStacLoader, CopernicusOceanSalinity109DepthValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher, cache_location: str | None = None):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        CopernicusOceanSalinity109DepthValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution, cache_location=cache_location)
        Logging.__init__(self, dataset_name=self.dataset_name)