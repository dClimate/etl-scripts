"""
ETL Pipeline for {name}

Usage:
    {script} [options] init
    {script} [options] append
    {script} [options] replace
    {script} interact

Options:
    -h --help         Show this screen.
    --timespan SPAN   How much data to load along the time axis. [default: 1M]
    --daterange RANGE   The date range to load.
    --overwrite       Allow data to be overwritten.
    --pdb             Drop into debugger on error.
"""

import code
import datetime
import itertools
import pdb
import sys

import docopt
import numpy

from dateutil.relativedelta import relativedelta

from dc_etl.fetch import Timespan
from dc_etl.pipeline import Pipeline

ONE_DAY = relativedelta(days=1)


def main(pipeline: Pipeline):
    args = _parse_args()
    try:
        cid = pipeline.loader.publisher.retrieve()
        if args["init"]:
            if cid and not args["--overwrite"]:
                raise docopt.DocoptExit(
                    "Init would overwrite existing data. Use '--overwrite' flag if you really want to do this."
                )
            timedelta = _parse_timedelta(args["--timespan"])
            remote_span = pipeline.fetcher.get_remote_timespan()
            load_end = _add_delta(remote_span.start, timedelta - ONE_DAY)
            load_span = Timespan(remote_span.start, min(load_end, remote_span.end))
            print(load_span)
            run_pipeline(pipeline, load_span, pipeline.loader.initial)

        elif args["append"]:
            if not cid:
                raise docopt.DocoptExit("Dataset has not been initialized.")

            # Get the timedelta which is like 4Y or 7Y
            timedelta = _parse_timedelta(args["--timespan"])
            # Get the remote timespan of the dataset
            remote_span = pipeline.fetcher.get_remote_timespan()
            # Get the existing dataset
            existing = pipeline.loader.dataset()
            # Get the last time value of the existing dataset
            existing_end = existing.time[-1].values
            # If the last time value of the existing dataset is greater than or equal to the last time value of the remote dataset
            # then there is no more data to load
            if existing_end >= remote_span.end:
                print("No more data to load.")
                return
            # Get the last time value of the existing dataset and add one day to it to get the start of the next load
            load_begin = _add_delta(existing_end, ONE_DAY)
            # Get the end of the next load
            load_end = _add_delta(load_begin, timedelta - ONE_DAY)
            # Get the timespan to load
            load_span = Timespan(load_begin, min(load_end, remote_span.end))
            run_pipeline(pipeline, load_span, pipeline.loader.append)

        elif args["replace"]:
            load_span = _parse_timestamp(args["--daterange"])
            run_pipeline(pipeline, load_span, pipeline.loader.replace)

        else:
            dataset = pipeline.loader.dataset()
            code.interact("Interactive Python shell. The dataset is available as 'ds'.", local={"ds": dataset})

    except:
        if args["--pdb"]:
            pdb.post_mortem()
        raise


def run_pipeline(pipeline, span, load):
    print(
        f"Loading {span.start.astype('<M8[s]').astype(object):%Y-%m-%d} "
        f"to {span.end.astype('<M8[s]').astype(object):%Y-%m-%d}"
    )
    sources = pipeline.fetcher.fetch(span)
    extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    combined = pipeline.transformer(pipeline.combiner(extracted))
    load(combined, span)


def _parse_args():
    script = sys.argv[0]
    if "/" in script:
        _, script = script.rsplit("/", 1)
    name = script[:-3] if script.endswith(".py") else script
    doc = __doc__.format(name=name, script=script)
    return docopt.docopt(doc)


def _parse_timedelta(s: str):
    try:
        if s.endswith("Y"):
            years = int(s[:-1])
            return relativedelta(years=years)
        if s.endswith("M"):
            months = int(s[:-1])
            return relativedelta(months=months)
    except:  # noqa: E722
        pass

    raise docopt.DocoptExit(f"Unable to parse timespan: {s}")

def _parse_timestamp(s: str) -> Timespan:
    # Sould be like 2022-01-01_2022-01-02
    try:
        # Split the string by underscore
        s = s.split("_")
        start = numpy.datetime64(s[0])
        end = numpy.datetime64(s[1])
        # Return the numpy datetime64
        return Timespan(start=start, end=end)
    except:  # noqa: E722
        pass

    raise docopt.DocoptExit(f"Unable to parse timestamp: {s}")

def _add_delta(timestamp, delta):
    # Trying to manipulate datetimes with numpy gets pretty ridiculous
    timestamp = timestamp.astype("<M8[ms]").astype(datetime.datetime)
    timestamp = timestamp + delta

    # We only need to the day precision for these examples
    return numpy.datetime64(f"{timestamp.year}-{timestamp.month:02d}-{timestamp.day:02d}")