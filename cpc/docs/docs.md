# Datasets
+ `precip-conus` Daily precipitation data for the continental United States. https://psl.noaa.gov/mddb2/showDataset.html?datasetID=45
+ `precip-global` Daily precipitation data for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html
+ `tmax` Daily maximum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html
+ `tmin` Daily minimum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html

# .env setup
TODO describe the null value

# ETL Diagram
The following is a an example of how the ETL for precip-conus runs. ETLs for the other 3 datasets run very similarly, so you can substitute `precip-conus/` in the diagram for one of the other datasets.

![Rendered diagram in SVG](./etl-diagram.svg)

# Run the ETL
To run a complete ETL for one of the datasets, use the `pipeline.sh` script.

e.g. for `tmax` you would run `bash pipeline.sh tmax`
