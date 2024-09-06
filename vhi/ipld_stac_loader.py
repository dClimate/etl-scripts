from dc_etl.load import Loader
from dc_etl.ipld.loader import IPLDPublisher

import ipldstore
import xarray
from dc_etl.fetch import Timespan
from dataset_manager.utils.metadata import Metadata
from dataset_manager.utils.store import IPLD
from dataset_manager.utils.logging import Logging
import numpy as np

class IPLDStacLoader(Loader, Metadata, Logging):
    """Use IPLD to store datasets."""
    # Needed for the STAC
    collection_name = "VHI"
    organization = "dClimate"
    dataset_name = "vhi"
    time_resolution = "weekly"

    @classmethod
    def _from_config(cls, config):
        config["publisher"] = config["publisher"].as_component("ipld_publisher")
        return cls(**config)

    def __init__(self, time_dim: str, publisher: IPLDPublisher):
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
