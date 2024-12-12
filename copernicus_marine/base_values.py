import datetime
import pathlib

class CopernicusOceanValues():
    """
    Copernicus's Ocean Physics datasets present temperatuers and other variables
     at various depth profiles. See the website for more information
    https://resources.marine.copernicus.eu/products
    """
    dataset_name = "copernicus_ocean"

    time_resolution = "daily"

    collection_name = "Copernicus_Marine"
    """
    Overall collection of data. Used for filling STAC Catalogue.
    """

    protocol = "file"
    """
    Remote protocol string for MultiZarrToZarr and Xarray to use when opening input files.
    'File' for local, 's3' for S3, etc.
    See fsspec docs for more details.
    """

    identical_dimensions = ["longitude"]
    """
    List of dimension(s) whose values are identical in all input datasets.
    This saves Kerchunk time by having it read these dimensions only one time, from the first input file
    """

    concat_dimensions = ["time"]
    """
    List of dimension(s) by which to concatenate input files' data variable(s)
        -- usually time, possibly with some other relevant dimension
    """

    data_var_dtype = "<f8"

    has_nans: bool = True
    """If True, disable quality checks for NaN values to prevent wrongful flags"""

    reanalysis_start_date = datetime.datetime(1993, 1, 1)
    """
    Actual dataset start date of the dataset in 1993
    """

    interim_reanalysis_start_date = datetime.datetime(2021, 7, 1)
    """
    Start of interim reanalysis data not covered by finalized reanalysis data
    """

    preliminary_lag_in_days = 2

    dataset_start_date = datetime.datetime(1950, 1, 1)

    def relative_path(self):
        return pathlib.Path("copernicus_ocean")


class CopernicusOceanGlobalPhysicsValues():

    dataset_start_date = datetime.datetime(1950, 1, 1)
    final_lag_in_days = 1095
    title = "Global Ocean Physics Reanalysis"
    dataset_name = "copernicus_ocean"
    data_var_dtype = "<f4"
    update_cadence = "daily"
    spatial_resolution = 1 / 12
    preliminary_lag_in_days = 2
    time_resolution = "daily"
    spatial_precision = 0.000001
    missing_value = 9.969209968386869e36
    bbox = [-180.0, -80.0, 179.92, 90.0]
    expected_nan_frequency = 0.302941550075308
    """=Valid for many shallow-depth datasets, must be re-specified for child classes of lower-depth classes"""

    def _dataset_parameters(self, analysis_type: str) -> tuple[str, str, str]:
        """
        Convenience method to return the correct server and dataset_id for requests to the OpenDAP API

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
            title = "Global Ocean Physics Reanalysis"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        elif analysis_type == "interim-reanalysis":
            dataset_id = "cmems_mod_glo_phy_myint_0.083deg_P1D-m"
            title = "Global Ocean Physics Reanalysis (Interim)"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        elif analysis_type == "analysis":
            dataset_id = f"cmems_mod_glo_phy-{self.data_var}_anfc_0.083deg_P1D-m"
            title = "Global Ocean Physics Analysis and Forecast"
            info_url = "https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description"  # noqa: E501
        return dataset_id, title, info_url

    def _info_url(self, analysis_type: str) -> str:
        """
        Convenience method to specify the appropriate URL string for finding information about a dataset

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            return "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        elif analysis_type == "analysis":
            return "https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description"  # noqa: E501
    
class CopernicusOceanSeaSurfaceHeightValues(CopernicusOceanValues):
    """Child class for Sea Surface Height datasets"""

    # Sea Surface Height dataset size is time: 10585, latitude: 668, longitude: 1440
    def __init__(self):
        self.dataset_name = f"{CopernicusOceanValues.dataset_name}_sea_level"
        self.requested_dask_chunks={"time": 400, "latitude": 144, "longitude": -1}  # 192.3 MB
        self.requested_zarr_chunks={"time": 200, "latitude": 144, "longitude": 16} # 2.14 MB
        self.requested_ipfs_chunker="size-10688"
        self.tags = ["Sea level anomaly, Sea surface height"]
        self.data_var = "sla"
        self.time_resolution = "daily"
        self.update_cadence = "irregular (approximately 5 months)"
        self.spatial_resolution = 0.125
        self.spatial_precision = 0.0001
        self.bbox =[-179.875, -89.875, 179.875, 89.875]
        self.missing_value = -2147483647
        self.unit_of_measurement = "m"
        self.standard_name = "sea_surface_height_above_geoid"
        self.long_name = "Sea Surface Height Above Geoid"
        self.final_lag_in_days = 150
        self.preliminary_lag_in_days = None
        self.expected_nan_frequency = 0.4226138117283951

    def relative_path(self):
        return super().relative_path() / "sea_level"



    def _dataset_parameters(self, analysis_type: str) -> tuple[str, str, str]:
        """
        Convenience method to return the correct dataset_id, title, and URL for querying the CDS API

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description"  # noqa: E501
            )
        elif analysis_type == "interim-reanalysis":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description"  # noqa: E501
            )
        elif analysis_type == "analysis":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES NRT"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_NRT_008_046/description"  # noqa: E501
            )
        return dataset_id, title, info_url

    def _info_url(self, analysis_type: str) -> str:
        """
        Convenience method to specify the appropriate URL string for finding information about a dataset

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            return "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_MY_008_047/INFORMATION"
        elif analysis_type == "analysis":
            return "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/INFORMATION"  # noqa: E501


class CopernicusOceanTemp0p5DepthValues(CopernicusOceanGlobalPhysicsValues):  # pragma: nocover
    """
    Copernicus Daily Ocean Temps at 0.5 meters of depth
    """
    def __init__(self):
        super().__init__()
        self.requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1}  # 1.09 MB
        self.requested_zarr_chunks={"time": 400, "latitude": 157, "longitude": 30}  # 7.53 MB
        self.requested_ipfs_chunker="size-18840"
        self.data_var = "thetao"
        self.time_resolution = "daily"
        self.standard_name = "sea_water_temperature"
        self.long_name = "Sea Water Temperature"
        self.tags = ["Temperature"]
        self.unit_of_measurement = "deg_C"
        self.scale_factor = 0.0007324442267417908
        self.add_offset = 21.0
        self.final_lag_in_days = 1095
        self.expected_nan_frequency = 0.302941550075308
        self.depth = 0.494025
        self.missing_value = 9.969209968386869e36
        self.dataset_name = f"{CopernicusOceanGlobalPhysicsValues.dataset_name}_temp_0p5_meters"

    def relative_path(self):
        return super().relative_path() / "temp" / "0p5_meters"

class CopernicusOceanTemp1p5DepthValues(CopernicusOceanGlobalPhysicsValues):  # pragma: nocover
    """
    Copernicus Daily Ocean Temps at 1.5 meters of depth
    """
    def __init__(self):
        super().__init__()
        self.requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1}  # 1.09 MB
        self.requested_zarr_chunks={"time": 400, "latitude": 157, "longitude": 30}  # 7.53 MB
        self.requested_ipfs_chunker="size-18840"
        self.data_var = "thetao"
        self.time_resolution = "daily"
        self.standard_name = "sea_water_temperature"
        self.long_name = "Sea Water Temperature"
        self.tags = ["Temperature"]
        self.unit_of_measurement = "deg_C"
        self.scale_factor = 0.0007324442267417908
        self.add_offset = 21.0
        self.final_lag_in_days = 1095
        self.expected_nan_frequency = 0.302941550075308
        self.depth = 1.494025
        self.missing_value = 9.969209968386869e36
        self.dataset_name = f"{CopernicusOceanGlobalPhysicsValues.dataset_name}_temp_1p5_meters"

    def relative_path(self):
        return super().relative_path() / "temp" / "1p5_meters"

class CopernicusOceanTemp6p5DepthValues(CopernicusOceanGlobalPhysicsValues):  # pragma: nocover

    def __init__(self):
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        super().__init__()
        self.data_var = "thetao"
        self.standard_name = "sea_water_temperature"
        self.long_name = "Sea Water Temperature"
        self.tags = ["Temperature"]
        self.time_resolution = "daily"
        self.unit_of_measurement = "deg_C"
        self.scale_factor = 0.0007324442267417908
        self.add_offset = 21.0
        self.final_lag_in_days = 1095
        # OceanTemp dataset size is time: 10,000+, latitude: 2041, longitude: 4320
        self.requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1}  # 2 GB
        self.requested_zarr_chunks={"time": 400, "latitude": 157, "longitude": 15} # 7 MB -- for some reason 6p5 works better w/ 15
        self.requested_ipfs_chunker="size-9420"
        self.depth = 6.440614
        self.missing_value = 9.969209968386869e36
        self.dataset_name = f"{CopernicusOceanGlobalPhysicsValues.dataset_name}_temp_6p5_meters"
        self.expected_nan_frequency = 0.302941550075308

    def relative_path(self):
        return super().relative_path() / "temp" / "6p5_meters"


class CopernicusOceanSalinityValues(CopernicusOceanGlobalPhysicsValues):  # pragma: nocover
    """
    Base class for Sea Water Salinity data
    """

    def __init__(self):
        """
        Initialize a new Copernicus Ocean Salinity object with appropriate chunking parameters.
        """
        super().__init__()
        # Salinity dataset sizes are time: ~10,000+, latitude: 2041, longitude: 4320
        self.requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1} # 2 GB
        self.requested_zarr_chunks={"time": 400, "latitude": 157, "longitude": 30} # 14 MB
        self.requested_ipfs_chunker="size-18840"
        self.dataset_name = f"{CopernicusOceanGlobalPhysicsValues.dataset_name}_salinity"
        self.data_var = "so"
        self.time_resolution = "daily"
        self.update_cadence = "daily"
        self.spatial_resolution = 1 / 12
        self.spatial_precision = 0.000001
        self.standard_name = "sea_water_salinity"
        self.long_name = "Sea Water Salinity"
        self.tags = ["Salinity"]
        self.missing_value = 9.969209968386869e36
        self.unit_of_measurement = ""
        """
        Copernicus specifies a Practical Salinity Unit (psu) unit
            that is specifically disrecommended by the scientific community.
        Astropy doesn't support it and this causes downstream issues with the API,
            therefore we specify an empty dimension
        """
        self.scale_factor = 0.0015259254723787308
        self.add_offset = -0.0015259254723787308
        self.final_lag_in_days = 1095

class CopernicusOceanSalinity0p5DepthValues(CopernicusOceanSalinityValues):  # pragma: nocover
    """
    Salinity data at 0.5 meters depth
    """
    def __init__(self):
        super().__init__()
        self.depth = 0.494025
        self.dataset_name = f"{self.dataset_name}_0p5_meters"

    def relative_path(self):
        return super().relative_path() / "0p5_meters"


class CopernicusOceanSalinity1p5DepthValues(CopernicusOceanSalinityValues):  # pragma: nocover
    """
    Salinity data at 1.5 meters depth
    """
    def __init__(self):
        super().__init__()
        print(self.dataset_name)
        self.depth = 1.541375
        self.dataset_name = f"{self.dataset_name}_1p5_meters"

    def relative_path(self):
        return super().relative_path() / "1p5_meters"


class CopernicusOceanSalinity2p6DepthValues(CopernicusOceanSalinityValues):  # pragma: nocover
    """
    Salinity data at 2.6 meters depth
    """
    def __init__(self):
        super().__init__()
        self.depth = 2.645669
        self.dataset_name = f"{self.dataset_name}_2p6_meters"

    def relative_path(self):
        return super().relative_path() / "2p6_meters"

class CopernicusOceanSalinity25DepthValues(CopernicusOceanSalinityValues):  # pragma: nocover
    """
    Salinity data at 25 meters depth
    """
    def __init__(self):
        super().__init__()
        self.depth = 25.21141
        self.dataset_name = f"{self.dataset_name}_25_meters"
        self.expected_nan_frequency = 0.31756208376431305

    def relative_path(self):
        return super().relative_path() / "25_meters"

class CopernicusOceanSalinity109DepthValues(CopernicusOceanSalinityValues):  # pragma: nocover
    """
    Salinity data at 109 meters depth
    """
    def __init__(self):
        super().__init__()
        self.depth = 109.7293
        self.dataset_name = f"{self.dataset_name}_109_meters"
        self.expected_nan_frequency = 0.35055596385214216

    def relative_path(self):
        return super().relative_path() / "109_meters"

