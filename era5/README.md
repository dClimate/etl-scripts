# ERA5 ETL Pipeline

## Available Datasets
- `precip`
- `solar-surface-downwards`
- `2m-temperature`
- `volumetric-soil-water-layer-1`
- `volumetric-soil-water-layer-2`
- `volumetric-soil-water-layer-3`
- `volumetric-soil-water-layer-4`
- `instantaneous-wind-gust-10m`
- `wind-u-10m`
- `wind-v-10m`
- `wind-u-100m`
- `wind-v-100m`
- `sea-surface-temperature`
- `sea-surface-temperature-daily`
- `sea-level-pressure`
- `land-precip`
- `land-dewpoint-temperature`
- `land-snowfall`
- `land-2m-temp`
- `land-surface-solar-radiation-downwards`
- `land-surface-pressure`
- `land-wind-u`
- `land-wind-v`

## Commands

### ETL Pipeline for `{name}`

```
Usage: 
    {script} [options] init 
    {script} [options] init-stepped 
    {script} [options] append 
    {script} [options] replace 
    {script} interact

Options: 
    -h --help                 Show this screen. 
    --timespan SPAN           How much data to load along the time axis. [default: 1M] 
    --daterange RANGE         The date range to load. 
    --overwrite               Allow data to be overwritten. 
    --pdb                     Drop into debugger on error. 
    --dataset DATASET         The dataset to run (e.g., "precip", "solar"). [default: precip]
```


### Command Details

- **`init-stepped`**  
  Loads the dataset in time steps, as specified by `--timespan`. 
  Example usage:

`python -m era5.era5 init-stepped --overwrite --dataset instantaneous-wind-gust-10m --timespan 1M`

This example downloads, converts, and uploads 1 month at a time, cleaning up between steps until finished.

- **`interact`**  
Downloads and uploads missing data. Useful for periodic updates, such as running the pipeline daily.

- **`append` & `replace`**  
These commands are for targeted updates, where you need to add or replace specific portions of the data. These are not typically required unless necessary for specialized tasks.

- **`init`**  
Initializes the entire dataset in one go. Generally, it's better to use `init-stepped` to avoid overwhelming resources.