How to setup your development environment
===

# Install system environment packages
+ `bash` version 5, for use of new builtins in like mapfile. Checking your bash version is important for macOS systems which are stuck on bash 3.
+ GNU coreutils
  + For developers on mac, there are some differences between the BSD versions used by macOS and the GNU ones on linux servers.
  + Mac users, make sure `date` is aliased to `gdate` if you install GNU coreutils.
+ `git` Both for downloading this repository's code and for downloading the `ipldstore` python dependency.
+ `shellcheck` This project makes extensive use of bash shell scripts to either build some of the ETL components or tie together ETL component python scripts. This can't be managed by `uv` so you have to install this separately. See the [shellcheck GitHub](https://github.com/koalaman/shellcheck?tab=readme-ov-file#installing).
+ [`uv`](https://github.com/astral-sh/uv) For installing python packages and creating a python virtual environment.
+ `kubo` the golang ipfs daemon.
+ `wget` For downloading dataset files for CPC, CHIRPS, PRISM.
+ `curl` For querying html pages for CHIRPS, PRISM.
+ `unzip` Unzips .nc.zip files downloaded for PRISM.

# Create python virtual environment
Running commands with `uv` normally automatically uses the virtual environment, but if you need to manually use it, it's located at `.venv`. To manually regenerate it, use `uv sync`.
