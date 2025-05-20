<p align="center">
<a href="https://dclimate.net/" target="_blank" rel="noopener noreferrer">
<img width="50%" src="https://user-images.githubusercontent.com/41392423/173133333-79ef15d0-6671-4be3-ac97-457344e9e958.svg" alt="dClimate logo">
</a>
</p>

This repository contains ETLs to IFPS for publicly available datasets.

# Dataset Standardization Practices
+ Zarr chunks are optimized for long timeseries calculations: they are wide in time and short in spatial
+ Chunking is set to 400 in time, 25 in latitude, 25 in longitude (for data variables that are float32, which is all of them)
  + With this setting, each uncompressed chunk is exactly 1 Megabyte in size
+ longitude is -180 to 180, latitude is -90 to 90. Both are sorted in ascending order.
+ Source file metadata, including netCDF scaling and additive shift variables (which are applied and then removed), is largely dropped

# Dataset Dashboard
[![cpc-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-availablity-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-availablity-check.yaml)
[![chirps-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-availability-check.yaml)
[![era5-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/era5-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/era5-availability-check.yaml)
[![prism-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/prism-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/prism-availability-check.yaml)

# Datasets
Each heading is a set of data variables from some source, with the data variables listed inside.

## CPC
+ `precip-conus` Daily precipitation data for the continental United States. https://psl.noaa.gov/mddb2/showDataset.html?datasetID=45
+ `precip-global` Daily precipitation data for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html
+ `tmax` Daily maximum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html
+ `tmin` Daily minimum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html

## CHIRPS
CHIRPS provides land precipitation estimates. For a more detailed overview, see https://data.chc.ucsb.edu/products/CHIRPS-2.0/README-CHIRPS.txt

"p05"/"p25" refers to the precision/width of the grid cells, where p05 means 0.05 lat/lon degrees and p25 means 0.25 lat/lon degrees.

(Based on [this site](https://www.chc.ucsb.edu/data) stating CHIRPS incorporates 0.05° resolution satellite imagery", we assume that's what p05 means.

A "final-" prefix refers to finalized data. As opposed to the  "prelim" prefix, which refers to data that may be revised by CHIRPS, but is often closer to realtime values.

+ `final-p05` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/
+ `final-p25` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25/
+ `prelim-p05` https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/netcdf/p05/

## ERA5
ERA5 provides a wide variety of hourly climate data variables and stretches back to 1940-01-01. See more here on the [Copernicus site.](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview)

The ETL is only configured to accept data variables dClimate is concerned with, but it is very easy to extend the list, other variables should work the same.

Running the ETL requires a S3 compatible bucket to cache raw data files, you can configure that by taking the template in `era5-env.json.example` and writing that to a `era5-env.json` file.

You can find the below variable pages and descriptions by starting your search from this [wiki page](https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#heading-Table1surfaceandsinglelevelparametersinvariantsintime).
+ [`2m_temperature`](https://codes.ecmwf.int/grib/param-db/167) "temperature of air at 2m above the surface of land, sea or in-land waters"
+ [`10m_u_component_of_wind`](https://codes.ecmwf.int/grib/param-db/165) "the eastward component of the 10m wind. It is the horizontal speed of air moving towards the east, at a height of ten metres above the surface of the Earth"
+ [`10m_v_component_of_wind`](https://codes.ecmwf.int/grib/param-db/166) "northward component of the 10m wind. It is the horizontal speed of air moving towards the north, at a height of ten metres above the surface of the Earth"
+ [`100m_u_component_of_wind`](https://codes.ecmwf.int/grib/param-db/228246) "eastward component of the 100 m wind. It is the horizontal speed of air moving towards the east, at a height of 100 metres above the surface of the Earth"
+ [`100m_v_component_of_wind`](https://codes.ecmwf.int/grib/param-db/228247) "northward component of the 100 m wind. It is the horizontal speed of air moving towards the north, at a height of 100 metres above the surface of the Earth"
+ [`surface_pressure`](https://codes.ecmwf.int/grib/param-db/134) "pressure (force per unit area) of the atmosphere on the surface of land, sea and in-land water"
+ [`surface_solar_radiation_downwards`](https://codes.ecmwf.int/grib/param-db/169) "mount of solar radiation (also known as shortwave radiation) that reaches a horizontal plane at the surface of the Earth. This parameter comprises both direct and diffuse solar radiation"
+ [`total_precipitation`](https://codes.ecmwf.int/grib/param-db/228) "accumulated liquid and frozen water, comprising rain and snow, that falls to the Earth's surface"

## PRISM
The [PRISM](https://prism.oregonstate.edu/) organization provides precipitation and temperature data for the continental United States.

PRISM creates both 800 meter and 4 km precision versions. Only the [4 km versions are free](https://prism.oregonstate.edu/orders/), so we ETL those.

+ `precip-4km`
+ `tmax-4km`
+ `tmin-4km`

## One-Shots
The `one-shots` folder contains ETLs for data that only needs to be ETLed once, and does not need to be maintained. This is why many of the ETLs are also added as Jupyter notebooks, for ease of use by data scientists exploring and molding the data before it is provided to dClimate for storage in our system.
