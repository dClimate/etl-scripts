<p align="center">
<a href="https://dclimate.net/" target="_blank" rel="noopener noreferrer">
<img width="50%" src="https://user-images.githubusercontent.com/41392423/173133333-79ef15d0-6671-4be3-ac97-457344e9e958.svg" alt="dClimate logo">
</a>
</p>

This repository contains ETLs to IFPS for publicly available datasets.

# Dataset Monitoring Dashboard
## Dataset Availability
[![cpc-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-availablity-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-availablity-check.yaml)

[![chirps-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-availability-check.yaml)

[![era5-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/era5-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/era5-availability-check.yaml)

## Datasets are up to date
[![cpc-check-uptodate](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-check-uptodate.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-check-uptodate.yaml)

[![chirps-check-uptodate](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-check-uptodate.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-check-uptodate.yaml)

# Dataset Standardization Practices
+ zarr chunks are wide in time, short spatially to allow for better long timeseries calculations
+ Chunking is set to 400 in time, 25 in latitude, 25 in longitude (for data variables that are float32)
  + With this setting, each chunk is exactly 1 Megabyte in size (400*25*25*(4 bytes per float32) = 1,000,000 bytes)
+ longitude is -180 to 180, latitude is -90 to 90. Both are sorted in ascending order.
+ Empty chunks are not written
+ Most metadata from source files are dropped. Note that this also includes the netCDF variables for scaling and additive shifts, which will be applied and then removed.

# Datasets

## CPC
+ `precip-conus` Daily precipitation data for the continental United States. https://psl.noaa.gov/mddb2/showDataset.html?datasetID=45
+ `precip-global` Daily precipitation data for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html
+ `tmax` Daily maximum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html
+ `tmin` Daily minimum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html

## CHIRPS
CHIRPS provides land precipitation estimates. For a more detailed overview, see https://data.chc.ucsb.edu/products/CHIRPS-2.0/README-CHIRPS.txt

The "p05"/"p25" suffix refers to the precision/width of the grid cells. Based on the fact that https://www.chc.ucsb.edu/data states "CHIRPS incorporates 0.05° resolution satellite imagery", we assume that the p05 and p25 directory at https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/ means a precision of 0.05 degrees and 0.25 degrees.

Final refers to finalized data. Prelim refers to data that may be changed by CHIRPS but is closer to realtime values.

+ `final-p05` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/
+ `final-p25` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25/
+ `prelim-p05` https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/netcdf/p05/

## ERA5
ERA5 provides a wide variety of hourly data variables and stretches back to 1940-01-01. See more here:
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview

The ETL is only configured to accept the data variables dClimate is concerned with, but it is very easy to extend the list, other variables should work the same.

Running the ETL requires a S3 compatible bucket to cache raw data files, you can configure that by taking the template in `era5-env.json.example` and writing that to a `era5-env.json` file.
