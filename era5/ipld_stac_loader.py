from dc_etl.load import Loader
from dc_etl.ipld.loader import IPLDPublisher

import ipldstore
import xarray
from dc_etl.fetch import Timespan
from dataset_manager.utils.metadata import Metadata
from dataset_manager.utils.store import IPLD
from dataset_manager.utils.logging import Logging
import numpy as np
from .base_values import ERA5PrecipValues, ERA52mTempValues, ERA5SurfaceSolarRadiationDownwardsValues, ERA5VolumetricSoilWaterLayer1Values, ERA5VolumetricSoilWaterLayer2Values, ERA5VolumetricSoilWaterLayer3Values, ERA5VolumetricSoilWaterLayer4Values, ERA5InstantaneousWindGust10mValues, ERA5WindU10mValues, ERA5WindV10mValues, ERA5WindU100mValues, ERA5WindV100mValues, ERA5SeaSurfaceTemperatureValues, ERA5SeaSurfaceTemperatureDailyValues, ERA5SeaLevelPressureValues, ERA5LandPrecipValues, ERA5LandDewpointTemperatureValues, ERA5LandSnowfallValues, ERA5Land2mTempValues, ERA5LandSurfaceSolarRadiationDownwardsValues, ERA5LandSurfacePressureValues, ERA5LandWindUValues, ERA5LandWindVValues

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

    def prepare_publish_stac_metadata(self, cid, dataset: xarray.Dataset, rebuild=False):
        """Prepare the STAC metadata for the dataset."""
        self.set_custom_latest_hash(str(cid))
        self.create_root_stac_catalog()
        self.create_stac_collection(dataset=dataset, rebuild=rebuild)
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
        dataset = self.set_zarr_metadata(dataset, overwrite=True)
        # Chunk the dataset to the requested dask chunks
        dataset = dataset.chunk(self.requested_dask_chunks)
        dataset.to_zarr(store=mapper, consolidated=True)
        cid = mapper.freeze()
        self.info("Preparing Stac Metadata")
        self.prepare_publish_stac_metadata(cid, dataset, rebuild=True)
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
        replace_dataset = replace_dataset.drop_vars([dim for dim in replace_dataset.dims if dim != self.time_dim])
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
        
class ERA5PrecipStacLoader(IPLDStacLoader, ERA5PrecipValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        # Initialize the logger with the dataset_name from ERA5PrecipValues
        ERA5PrecipValues.__init__(self)
        # Initialize the parent class IPLDStacLoader
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA52mTempStacLoader(IPLDStacLoader, ERA52mTempValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA52mTempValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5VolumetricSoilWaterLayer1StacLoader(IPLDStacLoader, ERA5VolumetricSoilWaterLayer1Values):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5VolumetricSoilWaterLayer1Values.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5VolumetricSoilWaterLayer2StacLoader(IPLDStacLoader, ERA5VolumetricSoilWaterLayer2Values):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5VolumetricSoilWaterLayer2Values.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5VolumetricSoilWaterLayer3StacLoader(IPLDStacLoader, ERA5VolumetricSoilWaterLayer3Values):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5VolumetricSoilWaterLayer3Values.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5VolumetricSoilWaterLayer4StacLoader(IPLDStacLoader, ERA5VolumetricSoilWaterLayer4Values):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5VolumetricSoilWaterLayer4Values.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5InstantaneousWindGust10mStacLoader(IPLDStacLoader, ERA5InstantaneousWindGust10mValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5InstantaneousWindGust10mValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5WindU10mStacLoader(IPLDStacLoader, ERA5WindU10mValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5WindU10mValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5WindV10mStacLoader(IPLDStacLoader, ERA5WindV10mValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5WindV10mValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5WindU100mStacLoader(IPLDStacLoader, ERA5WindU100mValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5WindU100mValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5WindV100mStacLoader(IPLDStacLoader, ERA5WindV100mValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5WindV100mValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5SeaSurfaceTemperatureStacLoader(IPLDStacLoader, ERA5SeaSurfaceTemperatureValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5SeaSurfaceTemperatureValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5SeaSurfaceTemperatureDailyStacLoader(IPLDStacLoader, ERA5SeaSurfaceTemperatureDailyValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5SeaSurfaceTemperatureDailyValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5SeaLevelPressureStacLoader(IPLDStacLoader, ERA5SeaLevelPressureValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5SeaLevelPressureValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandPrecipStacLoader(IPLDStacLoader, ERA5LandPrecipValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandPrecipValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandDewpointTemperatureStacLoader(IPLDStacLoader, ERA5LandDewpointTemperatureValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandDewpointTemperatureValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandSnowfallStacLoader(IPLDStacLoader, ERA5LandSnowfallValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandSnowfallValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5Land2mTempStacLoader(IPLDStacLoader, ERA5Land2mTempValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5Land2mTempValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandSurfaceSolarRadiationDownwardsStacLoader(IPLDStacLoader, ERA5LandSurfaceSolarRadiationDownwardsValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandSurfaceSolarRadiationDownwardsValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandSurfacePressureStacLoader(IPLDStacLoader, ERA5LandSurfacePressureValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandSurfacePressureValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandWindUStacLoader(IPLDStacLoader, ERA5LandWindUValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandWindUValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)

class ERA5LandWindVStacLoader(IPLDStacLoader, ERA5LandWindVValues):
    def __init__(self, time_dim: str, publisher: IPLDPublisher):
        ERA5LandWindVValues.__init__(self)
        super().__init__(time_dim=time_dim, publisher=publisher, dataset_name=self.dataset_name, time_resolution=self.time_resolution)
        Logging.__init__(self, dataset_name=self.dataset_name)
