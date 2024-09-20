import pathlib
import os

from .logging import Logging

from subprocess import Popen

class NCtoNetCDF4Converter(Logging):
    """
    Base class for transforming a collection of downloaded input files first into NetCDF4 Classic format
    """
    def __init__(self, dataset_name: str):
        super().__init__(dataset_name)

    # CONVERT FILES
    def parallel_subprocess_files(
        self,
        input_files: list[pathlib.Path],
        command_text: list[str],
        replacement_suffix: str,
        keep_originals: bool = False,
        invert_file_order: bool = False,
        
    ):
        """
        Run a command line operation on a set of input files. In most cases, replace each file with an alternative
        file.

        Optionally, keep the original files for development and testing purposes.

        Parameters
        ----------
        raw_files : list
            A list of pathlib.Path objects referencing the original files prior to processing
        command_text : list[str]
            A list of strings to reconstruct into a shell command
        replacement_suffix : str
            The desired extension of the file(s) created by the shell routine. Replaces the old extension.
        keep_originals : bool, optional
            An optional flag to preserve the original files for debugging purposes. Defaults to False.
        """
        # set up and run conversion subprocess on command line
        commands = []
        for existing_file in input_files:
            # CDO specifies the extension via an environment variable, not an argument...
            if "cdo" in command_text:
                new_file = existing_file.with_suffix("")
                os.environ["CDO_FILE_SUFFIX"] = replacement_suffix
            # ...but other tools use arguments
            else:
                new_file = existing_file.with_suffix(replacement_suffix)
            if invert_file_order:
                filenames = [new_file, existing_file]
            else:
                filenames = [existing_file, new_file]
            # map will convert the file names to strings because some command line tools (e.g. gdal) don't like Pathlib
            # objects
            commands.append(list(map(str, command_text + filenames)))
        # CDO responds to an environment variable when assigning file suffixes
        # Convert each command to a Popen call b/c Popen doesn't block, hence processes will run in parallel
        # Only run 100 processes at a time to prevent BlockingIOErrors
        for index in range(0, len(commands), 100):
            commands_slice = [Popen(cmd) for cmd in commands[index : index + 100]]
            for command in commands_slice:
                command.wait()
                # if not keep_originals:
                #     if not invert_file_order:
                #         os.remove(command.args[-2])
                #     else:
                #         os.remove(command.args[-1])

        self.info(f"{(len(input_files))} conversions finished, cleaning up original files")
        # Get rid of original files that were converted
        # if keep_originals:
        #     self.archive_original_files(input_files)
        self.info("Cleanup finished")

    def convert_to_lowest_common_time_denom(self, raw_files: list, keep_originals: bool = False):
        """
        Decompose a set of raw files aggregated by week, month, year, or other irregular time denominator
        into a set of smaller files, one per the lowest common time denominator -- hour, day, etc.

        Converts to a NetCDF4 Classic file as this has shown consistently performance for parsing

        Parameters
        ----------
        raw_files : list
            A list of file path strings referencing the original files prior to processing
        originals_dir : pathlib.Path
            A path to a directory to hold the original files
        keep_originals : bool, optional
            An optional flag to preserve the original files for debugging purposes. Defaults to False.

        Raises
        ------
        ValueError
            Return a ValueError if no files are passed
        """
        if len(raw_files) == 0:
            raise ValueError("No files found to convert, exiting script")
        command_text = ["cdo", "-f", "nc4c", "splitsel,1"]
        self.parallel_subprocess_files(
            input_files=raw_files,
            command_text=command_text,
            replacement_suffix=".nc4",
            keep_originals=keep_originals,
        )

    def ncs_to_nc4s(self, raw_files, keep_originals: bool = False):
        """
        Find all NetCDF files in the input folder and batch convert them
        in parallel to NetCDF4-Classic files that play nicely with Kerchunk

        NOTE There are many versions of NetCDFs and some others seem to play nicely with Kerchunk.
        NOTE To be safe we convert to NetCDF4 Classic as these are reliable and no data is lost.

        Parameters
        ----------
        keep_originals : bool
            An optional flag to preserve the original files for debugging purposes. Defaults to False.

        Raises
        ------
        ValueError
            Return a ValueError if no files are passed for conversion
        """
        # Build a list of files for manipulation, sorted so unit tests can have a consistent expected value
        if len(raw_files) == 0:
            raise FileNotFoundError("No files found to convert, exiting script")
        # convert raw NetCDFs to NetCDF4-Classics in parallel
        self.info(f"Converting {(len(raw_files))} NetCDFs to NetCDF4 Classic files")
        command_text = ["nccopy", "-k", "netCDF-4 classic model"]
        self.parallel_subprocess_files(
            input_files=raw_files, command_text=command_text, replacement_suffix=".nc4", keep_originals=keep_originals
        )

    def archive_original_files(self, files: list):
        """
        Move each original file to a "<dataset_name>_originals" folder for reference

        Parameters
        ----------
        files : list
            A list of original files to delete or save
        """
        # use the first file to define the originals_dir path
        first_file = files[0]
        originals_dir = first_file.parents[1] / (first_file.stem + "_originals")

        # move original files to the originals_dir
        originals_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        for file in files:
            file.rename(originals_dir / file.name)


