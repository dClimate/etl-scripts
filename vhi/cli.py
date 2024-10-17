"""
ETL Pipeline for {name}

Usage:
    {script} [options] init
    {script} [options] init-stepped
    {script} [options] append
    {script} [options] replace
    {script} interact

Options:
    -h --help         Show this screen.
    --timespan SPAN   How much data to load along the time axis. [default: 4W]
    --daterange RANGE   The date range to load.
    --overwrite       Allow data to be overwritten.
    --pdb             Drop into debugger on error.
"""

import datetime
import itertools
import pdb
import sys
import pandas as pd

import docopt
import numpy

from dateutil.relativedelta import relativedelta

from dc_etl.fetch import Timespan
from dc_etl.pipeline import Pipeline

ONE_WEEK = relativedelta(weeks=1)

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
            load_end = _add_delta(remote_span.start, timedelta - ONE_WEEK)
            load_span = Timespan(remote_span.start, min(load_end, remote_span.end))
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
            load_begin = _add_delta(existing_end, ONE_WEEK)
            # Get the end of the next load
            load_end = _add_delta(load_begin, timedelta - ONE_WEEK)
            # Get the timespan to load
            load_span = Timespan(load_begin, min(load_end, remote_span.end))
            run_pipeline(pipeline, load_span, pipeline.loader.append, args, manual_override=True)
            return

        elif args["replace"]:
            load_span = _parse_timestamp(args["--daterange"])
            run_pipeline(pipeline, load_span, pipeline.loader.replace, args, manual_override=True)
            return

        elif args["init-stepped"]:
            # Get the timescale to use (like 1 month, or any timedelta)
            timedelta = _parse_timedelta(args["--timespan"])
            
            # Get the remote timespan of the dataset (the full available time range)
            remote_span = pipeline.fetcher.get_remote_timespan()
            

            if not cid:
                # If the dataset does not exist, initialize the first timespan
                print("Dataset not found. Initializing dataset...")
                load_end = _add_delta(remote_span.start, timedelta - ONE_WEEK)
                load_span = Timespan(remote_span.start, min(load_end, remote_span.end))
                run_pipeline(pipeline, load_span, pipeline.loader.initial, args, manual_override=True)
                
                # Update the CID to mark the dataset as initialized
                cid = pipeline.loader.dataset()

                load_begin = _add_delta(load_span.end, ONE_WEEK)
                
            else:
                # If the dataset exists, pick up from where it left off
                print("Dataset exists. Resuming append process...")
                existing = pipeline.loader.dataset()
                existing_end = existing.time[-1].values

                # Check if there's more data to load
                if existing_end >= remote_span.end:
                    print("No more data to load.")
                    return
                
                # Calculate where to start appending from
                load_begin = _add_delta(existing_end, ONE_WEEK)
 
                existing_end_datetime = pd.to_datetime(existing_end)
                year = existing_end_datetime.year
                # Calculate remaining days until the end of the year
                remaining_days = (numpy.datetime64(f'{year}-12-31') - existing_end.astype('datetime64[D]')).astype(int)
                # If remaining days are more than 7 but less than 14, jump to the first day of the next year
                if 7 <= remaining_days < 14:
                    load_begin = numpy.datetime64(f'{year + 1}-01-01')

            # Continue appending in steps until the remote dataset end is reached
            while load_begin < remote_span.end:
                load_end = _add_delta(load_begin, timedelta)
                load_span = Timespan(load_begin, min(load_end, remote_span.end))
                run_pipeline(pipeline, load_span, pipeline.loader.append, args, manual_override=True)
                
                # Update load_begin for the next step (next month)
                load_begin = load_end
            
            return

        # If nothing is specified, run the pipeline for the entire date range
        run_pipeline(pipeline, None, pipeline.loader.replace, args, manual_override=False)

    except:
        if args["--pdb"]:
            pdb.post_mortem()
        raise


def run_pipeline(pipeline, span, load, args, manual_override=False):
    # If nothing exists, run the start for the entire date range
    new_span, pipeline_info = pipeline.assessor.start(args=args)

    # If there is a manual override, fetch, extract, transform, and load the data based on manual ovveride
    if manual_override:
        print(
            f"Loading {span.start.astype('<M8[s]').astype(object):%Y-%m-%d} "
            f"to {span.end.astype('<M8[s]').astype(object):%Y-%m-%d}"
        )
        sources = pipeline.fetcher.fetch(span, pipeline_info)
        extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
        combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
        load(combined, span)
        return
    
    existing_dataset = pipeline_info.get("existing_dataset", None)
    start, end = new_span

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
        if s.endswith("W"):
            weeks = int(s[:-1])
            return relativedelta(weeks=weeks)
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