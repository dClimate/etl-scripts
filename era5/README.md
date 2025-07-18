This document provides an overview of the ERA5 data processing pipeline, including the directory structure, available datasets, and instructions for running the command-line interface (CLI) commands.

-----

## 1\. Project Overview

This project is designed to **ETL (Extract, Transform, Load) ERA5 climate data**. It automates the downloading of GRIB files from the Copernicus Climate Data Store (CDS), processes them into a standardized Zarr format, and uploads them to an IPFS network for decentralized storage and access. The system is capable of handling both historical data and appending the latest updates, ensuring data integrity through a series of validation and verification checks.

-----

## 2\. Directory Structure

The project is organized into the following directories and files:

  * **`era5/`**: This is the main directory containing all the Python scripts for the ETL pipeline.
      * **`__init__.py`**: An empty file that marks the `era5` directory as a Python package.
      * **`cli.py`**: Defines the command-line interface (CLI) for interacting with the ETL pipeline, using the `click` library to create commands for building, appending, and processing datasets.
      * **`downloader.py`**: Manages the downloading of GRIB files from the Copernicus API and caching them to a Cloudflare R2 bucket.
      * **`processor.py`**: Contains the core logic for processing the downloaded GRIB files, including batch processing, appending data, and building full datasets.
      * **`standardizer.py`**: Standardizes the data by renaming variables, handling time coordinates, and chunking the dataset for efficient storage.
      * **`stac.py`**: Generates SpatioTemporal Asset Catalog (STAC) metadata for the processed datasets, making them discoverable and interoperable.
      * **`utils.py`**: Provides utility functions and constants used across the project, such as dataset names, chunking settings, and functions for interacting with the CDS API.
      * **`validator.py`**: Validates the downloaded GRIB files to ensure they contain the expected number of hourly timestamps for a given date range.
      * **`verifier.py`**: Verifies the integrity of the processed Zarr data by comparing it against the original GRIB files.
  * **`era5-env.json`**: A configuration file containing credentials for accessing the Cloudflare R2 bucket where the raw GRIB files are cached.
  * **`era5/cids.json`**: A file that stores the IPFS Content Identifiers (CIDs) for the processed datasets, along with finalization dates.

-----

## 3\. Available Datasets

The following ERA5 datasets can be processed using this pipeline:

  * `2m_temperature`
  * `10m_u_component_of_wind`
  * `10m_v_component_of_wind`
  * `100m_u_component_of_wind`
  * `100m_v_component_of_wind`
  * `surface_pressure`
  * `surface_solar_radiation_downwards`
  * `total_precipitation`

-----

## 4\. How to Run Commands

The command-line interface (CLI) provides several commands for processing the ERA5 data. These commands are defined in `era5/cli.py` and can be executed from the command line.

### Build a Dataset

To build a new dataset from scratch, use the `build-dataset` command. This command will download the data for the specified dataset, process it, and create a new IPFS Zarr store.

Validation is buggy so you need to include your apikey or include in the env as CDS_API_KEY Build dataset is a very long running process. It may take several hours.
It will also only return the last full chunked possible end date. This means you will gave to call the append command to finish it.

Finalizition only will only process finalized data from era5.

```bash
uv run era5/cli.py build-dataset <DATASET_NAME> --api-key <APIKEY> --finalization-only <BOOL>
```

**Example:**

```bash
uv run era5/cli.py build-dataset 2m_temperature --api-key <APIKEY>
```

### Append to a Dataset

To append new data to an existing dataset, use the `append` command. This command requires the CID of the existing dataset.

BE MINDFUL OF FINALIZATION ONLY. 

If the cid is finalized data only, append should also run with the same command to prevent mixing two data qualities.

```bash
uv run era5/cli.py append <DATASET_NAME> <CID> --api-key <APIKEY>
```

**Example:**

```bash
uv run era5/cli.py append 2m_temperature bafyr4icrox4pxashkfmbyztn7jhp6zjlpj3bufg5ggsjux74zr7ocnqdpu --api-key <APIKEY>
```

### Process a Batch of Data

To process a single batch of data for a specific date range, use the `process-batch` command.

```bash
uv run era5/cli.py process-batch <DATASET_NAME> --start-date <ISODATE> --end-date <ISODATE> --api-key <APIKEY>
```

**Example (Date Only):**

```bash
uv run era5/cli.py process-batch 2m_temperature --start-date 2024-01-01 --end-date 2024-01-31 --api-key <APIKEY>
```

**Example (Date and Time):**

```bash
uv run era5/cli.py process-batch 2m_temperature --start-date 2024-01-01T06:00:00 --end-date 2024-01-01T18:00:00 --api-key <APIKEY>
```