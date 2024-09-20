from dc_etl.load import Loader
from dc_etl.ipld.loader import IPLDPublisher

import ipldstore
import xarray
from dc_etl.fetch import Timespan
from dataset_manager.utils.metadata import Metadata
from dataset_manager.utils.store import IPLD
from dataset_manager.utils.logging import Logging
import numpy as np
from .base_values import ERA5SurfaceSolarRadiationDownwardsValues

class IPLDStacLoader(Loader, Metadata, Logging):
    """Use IPLD to store datasets."""
    # Needed for the STAC
    collection_name = "ERA5"
    organization = "dClimate"
    use_compression = True
    encryption_key = None
    dataset_name =  None
    time_resolution = None

        

    @classmethod
    def _from_config(cls, config):
        config["publisher"] = config["publisher"].as_component("ipld_publisher")
        return cls(**config)

    def __init__(self, time_dim: str, publisher: IPLDPublisher, dataset_name: str, time_resolution: str):
        super().__init__(host="http://127.0.0.1:5001")
        self.time_dim = time_dim
        self.publisher = publisher
        metadata = {
            "title": "ECMWF ERA5-Land Reanalysis",
            "license": "Apache License 2.0",
            "provider_description": "ECMWF is the European Centre for Medium-Range Weather Forecasts. It is both a research institute and a 24/7 operational service, producing global numerical weather predictions and other data for its Member and Co-operating States and the broader community. The Centre has one of the largest supercomputer facilities and meteorological data archives in the world. Other strategic activities include delivering advanced training and assisting the WM in implementing its programs.",
            "provider_url": "https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview",
            "terms_of_service": "https://www.ecmwf.int/en/terms-use",
            "coordinate_reference_system": "Reduced Gaussian Grid",
            "organization": "Copernicus Climate Change Service (C3S)",
            "publisher": "Copernicus Climate Change Service (C3S)",
        }
        IPLDStacLoader.dataset_name = dataset_name
        IPLDStacLoader.time_resolution = time_resolution
        self.store = IPLD(self)
        self.host = "http://127.0.0.1:5001" 
        self.metadata = metadata

    def static_metadata(self):
        return self.metadata

    def prepare_publish_stac_metadata(self, cid, dataset: xarray.Dataset):
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
        self.prepare_publish_stac_metadata(cid, dataset)
        self.publisher.publish(cid)
        self.info(f"Published {cid}")

    def append(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Append data to an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
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

    def replace(self, replace_dataset: xarray.Dataset, span: Timespan | None = None):
        """Replace a contiguous span of data in an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        original_dataset = self.dataset()
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

class ERA5SurfaceSolarRadiationDownwardsStacLoader(IPLDStacLoader, ERA5SurfaceSolarRadiationDownwardsValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        # Initialize the logger with the dataset_name from ERA5SurfaceSolarRadiationDownwardsValues
        ERA5SurfaceSolarRadiationDownwardsValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)
        