This is a list of all the parts of the repository, and what their purpose is.

For more specific documentation on a dataset, see the `docs/` inside that dataset's folder.

# `.gitignore`
This file contains ignore rules common to all datasets. However, since datasets are kept as independent as possible, anything specific to only one dataset is kept inside the `.gitignore` in that dataset's folder.

# `.pre-commit-config.yaml`
This specifies the pre commit hooks to run. For more information about the pre-commit library, see https://pre-commit.com/

# `.python-version`
This file specifies the virtual environment python version to use for `uv`. This can also be specified as a command line argument, but we thought it was best to specify it explicitly in version control and remove the need for writing it every time.

# `.pyproject.toml`
We still use a `pyproject.toml` since this allows to easily list dependencies and optional dependencies in a format understood by the wider community as well as `uv`.

# `README.md`
Mainly meant as very basic introductory information visitors browsing on GitHub.

# `development.md`
Instructions for setting up the development environment to work on these ETLs.

This is separate from running these ETLs on your own IPFS Cluster! For example, you would not want to install development tools like ruff or pre-commit on the cluster nodes. We have our own specific deployment practices for our production environment, but you can use just setting up the development environment on your nodes as a starting point.

# `monitoring/`
This folder contains scripts used for collecting metrics on the running cluster, such as time to first load in xarray from a CID.

# `operations/`
Utilities for running the cluster.

# `shared_python_scripts/`
Python scripts that are utilized across multiple ETLs.

# Dataset Directories
Each of these following directories correspond to an organization that produces multiple datasets.
## `cpc/`
CPC stands for Climate Prediction Center. It's an organization that "collects and produces daily and monthly data, time series, and maps for various climate parameters, such as precipitation, temperature, snow cover, and degree days for the United States, Pacific Islands, and other parts of the world.”
source: https://www.cpc.ncep.noaa.gov/products/monitoring_and_data/

## `chirps/`
"Climate Hazards Group InfraRed Precipitation with Station data (CHIRPS) is a 35+ year quasi-global rainfall data set. Spanning 50°S-50°N (and all longitudes) and ranging from 1981 to near-present"
source: https://www.chc.ucsb.edu/data/chirps

## `prism/`
"The PRISM Climate Group gathers climate observations from a wide range of monitoring networks"
source: https://www.prism.oregonstate.edu/
