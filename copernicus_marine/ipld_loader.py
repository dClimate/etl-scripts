from dc_etl.load import Loader
from dc_etl.ipld.loader import IPLDPublisher

import ipldstore
import xarray
from dc_etl.fetch import Timespan
from dataset_manager.utils.metadata import Metadata
from dataset_manager.utils.ipld_store import IPLDStore
from dataset_manager.utils.store import IPLD
from dataset_manager.utils.ipfs import IPFS

class IPLDStacLoader(Loader, Metadata, IPLDStore, IPFS):
    """Use IPLD to store datasets."""
    collection_name = "Copernicus Marine"
    organization = "dClimate"
    dataset_name = "copernicus_sea_level"
    time_resolution = "daily"

    @classmethod
    def _from_config(cls, config):
        config["publisher"] = config["publisher"].as_component("ipld_publisher")
        return cls(**config)

    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        IPFS.__init__(self, host="http://127.0.0.1:5001")
        super().__init__()
        self.time_dim = time_dim
        self.publisher = publisher
        metadata = {
            "title": "Copernicus Marine Sea LeveL",
            "license": "CC-BY-4.0",
            "provider_description": "Copernicus Marine",
            "provider_url": "https://marine.copernicus.eu/",
            "terms_of_service": "https://marine.copernicus.eu/services-portfolio/service-commitments/",
            "coordinate_reference_system": "EPSG:4326",
            "organization": "Copernicus Marine",
            "publisher": "Copernicus Marine",
        }

        self.store = IPLD(self)
        self.data_var = "sla"
        self.custom_latest_hash = None

        self.host = "http://127.0.0.1:5001" 
        self.requested_ipfs_chunker = "size-10688"
        # self.collection_name = "Test"
        # self.organization = "dClimate"
        self.metadata = metadata

    def static_metadata(self):
        return self.metadata

    def initial(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Start writing a new dataset."""



        mapper = self._mapper()
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        dataset.to_zarr(store=mapper, consolidated=True)
        cid = mapper.freeze()

        self.custom_latest_hash = str(cid)
        print(f"Published {cid}")
        # Create the Stack catalog
        self.create_root_stac_catalog()
        self.create_stac_collection(dataset=dataset)
        self.create_stac_item(dataset=dataset)

        self.publisher.publish(cid)

    def append(self, dataset: xarray.Dataset, span: Timespan | None = None):
        """Append data to an existing dataset."""
        mapper = self._mapper(self.publisher.retrieve())
        dataset = dataset.sel(**{self.time_dim: slice(*span)})
        dataset.to_zarr(store=mapper, consolidated=True, append_dim=self.time_dim)
        cid = mapper.freeze()
        self.publisher.publish(cid)

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
        replace_dataset.to_zarr(store=mapper, consolidated=True, region={self.time_dim: slice(*region)})

        cid = mapper.freeze()
        self.publisher.publish(cid)

    def dataset(self) -> xarray.Dataset:
        """Convenience method to get the currently published dataset."""
        mapper = self._mapper(root=self.publisher.retrieve())
        return xarray.open_zarr(store=mapper, consolidated=True)

    def _mapper(self, root=None):
        mapper = ipldstore.get_ipfs_mapper()
        if root is not None:
            mapper.set_root(root)
        return mapper

    def _time_to_integer(self, dataset, timestamp):
        # It seems like an oversight in xarray that this is the best way to do this.
        nearest = dataset.sel(**{self.time_dim: timestamp, "method": "nearest"})[self.time_dim]
        return list(dataset[self.time_dim].values).index(nearest)
