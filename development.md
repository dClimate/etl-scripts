TODO warning about storage space estimates for each dataset, include the numbers here from Notion

TODO explain this motif that's in pretty much every shell script
# Change to this script's directory
# This allows us to call this script from anywhere
cd $(dirname "$0")

TODO include section about adding gdate to bashrc for macOS developers, but still having gdate available for noninteractive subshells

TODO note info about the naming schemes of things

TODO note about the blackhole-webhook.py and how that is used for .env in each of the dataset's

# How to setup your development environment
## Step 1: Install system environment packages
Ensure the following are installed in the local environment, manually or through your package manager.
+ `bash` version 4 or greater, for use of new builtins in like mapfile. Checking your bash version is important for macOS systems which are stuck on bash 3.
+ `git` Both for downloading this repository's code and for downloading the `ipldstore` python dependency.
+ `uv` https://github.com/astral-sh/uv
  + For installing python packages and creating a python virtual environment
+ `kubo` the ipfs daemon written in go. [https://dist.ipfs.tech/#kubo](https://dist.ipfs.tech/#kubo)
+ `ipfs-cluster-service` for running the ipfs-cluster daemon. [https://dist.ipfs.tech/#ipfs-cluster-service](https://dist.ipfs.tech/#ipfs-cluster-service)
  + There is another daemon available called ipfs-cluster-follow, this can follow pinsets but not modify them, so it is not useful in our case.
+ `ipfs-cluster-ctl` for interfacing with the ipfs-cluster. https://dist.ipfs.tech/#ipfs-cluster-ctl
+ `wget` For downloading dataset files for CPC, CHIRPS, PRISM, VHI
+ `curl` For querying html pages for CHIRPS, PRISM, VHI
+ `unzip` Unzips .nc.zip files downloaded for PRISM
+ Standard POSIX tools like grep, sed, awk

## Step 2: Create python virtual environment
1. Use `uv` to instantiate the virtual environment and install packages
```sh
# pwd = ....../etl-scripts/
uv venv
uv pip compile --all-extras pyproject.toml -o requirements.txt
uv pip sync requirements.txt
```
2. To activate the virtual environment:
```sh
source .venv/bin/activate
```
To deactivate once done working, just run
```sh
deactivate
```

## Step 3: Setup IPFS Daemon
Follow the documentation for the kubo ipfs daemon to set it up and run it

## Step 4: Start ipfs-cluster
+ First, ensure your ipfs daemon is running. See documentation here https://docs.ipfs.tech/how-to/command-line-quick-start/
+ Now ensure that your ipfs-cluster-service daemon is running. See documentation here https://ipfscluster.io/documentation/deployment/setup/

To check that your ipfs cluster on your machine is running and can talk to your ipfs daemon, run
```sh
$ ipfs-cluster-ctl id
```

# Install Commit Hooks
This commit hook just checks formatting and linting before you commit. You can inspect the config in the `.pre-commit-config.yaml` file.
```sh
$ source .venv/bin/activate
(venv) $ pre-commit install
```

# Formatting and Linting
Just run the pre-commit hook using
```sh
(venv) $ pre-commit run --all-files
```
This will reformat and lint all files.

## Manually Formatting
```sh
(venv) $ ruff format
```
This command automatically reformats. To only do a check, do `ruff format --check`

## Manually Linting
```sh
(venv) $ ruff check
```

# Final Words
You're now ready to get developing! We recommend looking at the ETLs for CPC in `cpc/` as a good starting point.
