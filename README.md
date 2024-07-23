These are a set of scripts meant to perform the ETLs for dClimate. Almost all scripts are **idempotent**. This means that you can rerun the script and it will only change things if any of the underlying data has changed.

Necessary exceptions have been made however, and are noted where possible. Idempotency can be assumed for all the rest.

# Operations Setup
## Requirements
1. Ensure the following are installed in the local environment.
  + `wget`
  + GNU `parallel`
  + `uv` [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv) for python packages and virtual environment creation.
  + python >= 3.10.14

# Development Environment
## Setting up
### Local Environment
Ensure the following are installed in the local environment.
  + Everything from the Operations Setup requirements
  + `ruff` for python formatting defaults and linting. [https://github.com/astral-sh/ruff](https://github.com/astral-sh/ruff)
### Setup python virtual environment
1. Create virtual environment, install packages
```sh
cd ~/etl-scripts # or wherever this is
pwd # "..."/etl-scripts
uv venv
uv pip sync requirements.txt
```
`uv` will use `./.python-version` to determine what python version to install.

Now activate the virtual environment.
```sh
source .venv/bin/activate
```
To deactivate once done working, just run
```sh
deactivate
```

## Formatting and Linting
### Formatting
```sh
ruff format
```
This command automatically reformats any files as needed. To only do a check, run `ruff format --check`

### Linting
```sh
pwd # .../etl-scripts
ruff check
```

## Changing python requirements
We manually all dependencies in `pyproject.toml`, just like a package.json, but synchronize our actual installed complete tree of packages with `requirements-lock.txt`, which is generated from the `pyproject.toml` file.
### Add dependency
As an example, we will use `xarray` with the optional `[io]`.
1. Change `pyproject.toml` file.
```diff
diff --git a/pyproject.toml b/pyproject.toml
index 1234567..8901234 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -1,4 +1,5 @@
 dependencies = [
+    "xarray[io]",
     "ipldstore @ git+https://github.com/dClimate/ipldstore",
 ]
```
2. Create new locked set of dependencies `requirements-lock.txt`
```sh
uv pip compile pyproject.toml -o requirements-lock.txt
```
3. Reinstall and uninstall package as needed, all computed automatically by `uv`
```sh
uv pip sync requirements-lock.txt
```

### Remove a dependency
It's the same steps, regenerate `requirements-lock.txt` and then `uv pip sync requirements-lock.txt`.
