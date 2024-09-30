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
    --dataset DATASET    The dataset to run (e.g., "precip", "solar"). [default: precip]
"""

import datetime
import itertools
import pdb
import sys
import time

import docopt
import numpy
import numpy as np

from dateutil.relativedelta import relativedelta

from dc_etl.fetch import Timespan
from dc_etl.pipeline import Pipeline

ONE_DAY = relativedelta(days=1)


def adjust_span_to_end_hour_23(span):
    # Check if the end time already ends at hour 23
    end_hour = span.end.astype('datetime64[h]').astype(int) % 24  # Extract the hour
    if end_hour != 23:
        # Adjust the span so it ends at hour 23 of the same day
        end_of_day = np.datetime64(span.end, 'D') + np.timedelta64(23, 'h')
        span = Timespan(start=span.start, end=end_of_day)
    
    return span

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
            load_span = adjust_span_to_end_hour_23(load_span)
            print(load_span)
            run_pipeline(pipeline, load_span, pipeline.loader.initial, args, manual_override=True)
            return

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
            run_pipeline(pipeline, load_span, pipeline.loader.append, args, manual_override=True)
            return

        elif args["replace"]:
            load_span = _parse_timestamp(args["--daterange"])
            run_pipeline(pipeline, load_span, pipeline.loader.replace, args, manual_override=True)
            return

        print("HERE")
        time.sleep(10)
        # If nothing is specified, run the pipeline for the entire date range
        run_pipeline(pipeline, None, pipeline.loader.replace, args, manual_override=False)

    except:
        if args["--pdb"]:
            pdb.post_mortem()
        raise


def run_pipeline(pipeline, span, load, args, manual_override=False):
    if span is not None:
        print(
            f"Loading {span.start.astype('<M8[s]').astype(object):%Y-%m-%d} "
            f"to {span.end.astype('<M8[s]').astype(object):%Y-%m-%d}"
        )
    # If nothing exists, run the start for the entire date range
    new_span, pipeline_info = pipeline.assessor.start(args=args)
    existing_dataset = pipeline_info.get("existing_dataset", None)

    start, end, new_finalization_date_start, new_finalization_date_end = new_span

    # If there is a manual override, fetch, extract, transform, and load the data based on manual ovveride
    if manual_override:
        sources = pipeline.fetcher.fetch(span, pipeline_info)
        extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
        combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
        load(combined, span)
        return
    
    # If there is a start and end date and there is no existing data, fetch, extract, transform, and load the data
    # Any finalization data will be passed via the pipeline info
    if (start and end and not existing_dataset):
        new_span = Timespan(start=start, end=end)
        loader = pipeline.loader.initial
        sources = pipeline.fetcher.fetch(new_span, pipeline_info)
        extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
        combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
        loader(combined, new_span)
        return
        
    # If something exists already append it first
    if existing_dataset:
        append_span = Timespan(start=start, end=end)
        loader = pipeline.loader.append
        sources = pipeline.fetcher.fetch(append_span, pipeline_info)
        extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
        combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
        loader(combined, append_span)

    # If there is a new finalization date, replace the data
    if (new_finalization_date_start and new_finalization_date_end):
        replace_span = Timespan(start=new_finalization_date_start, end=new_finalization_date_end)
        loader = pipeline.loader.replace
        sources = pipeline.fetcher.fetch(replace_span, pipeline_info)
        extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
        combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
        loader(combined, replace_span)



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