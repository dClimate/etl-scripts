# Operations Setup
## Requirements
1. Ensure the following are installed in the local environment.
  + `git`
  + `wget`
  + `uv` [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv) for python packages and virtual environment creation.
  + `python` >= 3.10.14

## Run an ETL for: CPC
```sh
cd ./cpc
sh pipeline.sh precip-conus precip-global tmax tmin
cat precip-conus/*.cid
cat precip-global/*.cid
cat tmax/*.cid
cat tmin/*.cid
```
The CIDs are stored as files in the directory created for each dataset from CPC once done.
You can also mix and match, e.g.
```sh
cd ./cpc
sh pipeline.sh tmin tmax
cat tmax/*.cid
cat tmin/*.cid
```
