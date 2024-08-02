# Development Environment Setup
## Local Environment
First ensure the following are installed.
+ `uv` [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv) for python packages and virtual environment creation.
+ python >= 3.10.14

## Setup python virtual environment
1. Use `uv`
```sh
$ pwd
"..."/etl-scripts
$ uv venv
```
2. To activate the virtual environment:
```sh
$ source .venv/bin/activate
```
To deactivate once done working, just run
```sh
$ deactivate
```

## Install Packages into virtual environment
```sh
$ uv pip compile --all-extras pyproject.toml -o requirements.txt
$ uv pip sync requirements.txt
```

## Install Commit Hooks
```sh
$ source .venv/bin/activate
(venv) $ pre-commit install
```

## Formatting and Linting
Just run the pre-commit hook using
```sh
pre-commit run --all-files
```
This will reformat all files, and lint them as well.
### Manually Formatting and Linting
```sh
$ ruff format
```
This command automatically reformats any files as needed. To only do a check, run `ruff format --check`

### Manually Linting
```sh
$ pwd
"..."/etl-scripts
$ ruff check
```

## Changing python requirements
### Add dependency
Pretend we are intalling the package `foo`.
1. Change `pyproject.toml` file.
```diff
diff --git a/pyproject.toml b/pyproject.toml
index 1234567..8901234 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -1,4 +1,5 @@
 dependencies = [
+    "foo",
     "ipldstore @ git+https://github.com/dClimate/ipldstore",
 ]
```
2. Now just rerun the steps to install packages. `uv` will automatically compute what to uninstall and reinstall for us.

### Remove a dependency
It's the same steps adding a dependency, after changing the `pyproject.toml` file, you rerun the steps to install packages.
