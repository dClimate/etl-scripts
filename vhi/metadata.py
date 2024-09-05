import datetime



def static_metadata():
    """
    dict containing static fields in the metadata
    """
    static_metadata = {
        "coordinate_reference_system": "EPSG:32662",
        "spatial_resolution": 360 / 10000,
        "spatial_precision": 0.01,
        "temporal_resolution": "weekly",
        "update_cadence": "weekly",
        "bbox": [-180, -55.152, 180, 75.024],
        "provider_url": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/index.php",
        "data_download_url": "https://www.star.nesdis.noaa.gov/data/pub0018/VHPdata4users/data/Blended_VH_4km/VH",
        "publisher": "DOC/NOAA/NESDIS/STAR > VHP Team, Center for Satellite Applications and Research (STAR)"
        " National Environmental Satellite, Data, and Information Service (NOAA-NESDIS)"
        " National Oceanic and Atmospheric Administration (NOAA)"
        " Department of Commerce",
        "title": "Global and Regional Vegetation Health Index (VHI)",
        "provider_description": "The National Ocean and Atmospheric Administration's (NOAA) Center for Satellite Applications"  # noqa: E501
        "and Research (STAR) uses innovative science and applications to transform satellite observations of the earth into"  # noqa: E501
        " meaninguful information essential to society's evolving environmental, security, and economic decision-making.",  # noqa: E501
        "dataset description": (
            "This dataset contains Vegetation Health Indices (VHI) derived from the radiance observed by the"  # noqa: E501
            " Advanced Very High Resolution Radiometer (AVHRR) onboard afternoon polar-orbiting satellites."  # noqa: E501
            "The VH products from AVHRR were produced from the NOAA/NESDIS Global Area Coverage (GAC) data set for the period"  # noqa: E501
            " 1981 to the present. The data and images have 4 km spatial and 7-day composite temporal resolution."  # noqa: E501
            "VH products from VIIRS were also processed from 2012 to present (1km resolution, 7 day composite)."  # noqa: E501
            "The VH indices range from 0 to 100 characterizing changes in vegetation conditions from extremely poor (0) to excellent (100)."  # noqa: E501
            " The VH reflects indirectly a combination of chlorophyll and moisture content in the vegetationhealth and also changes in thermal"  # noqa: E501
            " conditions at the surface.Monitoring vegetation health (condition), including drought detection and watch, is based on radiance"  # noqa: E501
            " measurements in the visible (VIS), near infrared (NIR), and 10.3-11.3 micrometers thermal (T) bands (channels) of the AVHRR."  # noqa: E501
            "The VH products can be used as proxy data for monitoring vegetation health, drought, soil saturation, moisture and"  # noqa: E501
            " thermal conditions, fire risk, greenness of vegetation cover, vegetation fraction, leave area index, start/end of the"  # noqa: E501
            " growing season, crop and pasture productivity, teleconnection with ENSO, desertification, mosquito-borne diseases,"  # noqa: E501
            " invasive species, ecological resources, land degradation, etc."  # noqa: E501
            "More dataset information at https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH_doc/VHP_uguide_v2.0_2018_0727.pdf"  # noqa: E501
        ),
        "license": "Public",
        "terms_of_service": "https://www.ngdc.noaa.gov/ngdcinfo/privacy.html",
        "name": "vhi",
        "updated": str(datetime.datetime.now()),
        "missing_value": -999,
        "tags": ["index", "vegetation"],
        "standard_name": "vegetation",
        "long_name": "Vegetation Health Index",
        "unit_of_measurement": "vegetative health score",
        "final_lag_in_days": 21,
        "expected_nan_frequency": 1,
    }

    return static_metadata