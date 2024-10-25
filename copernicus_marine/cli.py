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
    --timespan SPAN   How much data to load along the time axis. [default: 1Y]
    --daterange RANGE   The date range to load.
    --overwrite       Allow data to be overwritten.
    --pdb             Drop into debugger on error.
    --dataset DATASET    The dataset to run (e.g., "sea-surface-height"). [default: sea-surface-height]
"""

from datetime import datetime
import itertools
import pdb
import sys
import time  # noqa: F401

import docopt
import numpy

from dateutil.relativedelta import relativedelta

from dc_etl.fetch import Timespan
from dc_etl.pipeline import Pipeline

ONE_DAY = relativedelta(days=1)



# It takes the given time span and compares it against the reanalysis, interim-analysis, and analysis timespans.
# The process involves fetching data for each dataset in the order: reanalysis, interim-analysis, and analysis.
# For each dataset, it will extract, transform, and load the data.
# If no existing dataset is found, it will initialize with the reanalysis data. Otherwise, it will append new data.
# If a dataset already exists, it will replace the data for incomplete time spans.
def split_into_three_requests(pipeline, span, cid, args):
    # Retrieve the timespans for reanalysis, interim-analysis, and analysis
    reanalysis_time_span, interim_analysis_time_span, analysis_time_span = pipeline.assessor.get_all_timespans()
    existing_end = None
    if cid:
        existing_end = pipeline.assessor.get_end_date()

    first_run = True
    start_date = reanalysis_time_span.start
    if (existing_end and existing_end > start_date):
        first_run = False
        start_date = existing_end.astype('M8[D]').astype(datetime) + ONE_DAY
        start_date = numpy.datetime64(start_date)

    # Process the reanalysis data
    while start_date <= reanalysis_time_span.end:
        # Set the end date as the end of the year for the current start date
        end_date = numpy.datetime64(f"{start_date.astype('datetime64[Y]').astype(object).year}-12-31")
        load_span = Timespan(start_date, min(end_date, reanalysis_time_span.end))

        if first_run:
            # Initialize the pipeline for the first reanalysis dataset load
            run_pipeline(pipeline, load_span, pipeline.loader.initial, args, dataset_type="reanalysis")
            first_run = False
        else:
            # Append data for subsequent loads
            run_pipeline(pipeline, load_span, pipeline.loader.append, args, dataset_type="reanalysis")

        # Move to the next year
        start_date = numpy.datetime64(f"{start_date.astype('datetime64[Y]').astype(object).year + 1}-01-01")
    # Process the interim-analysis data
    interim_start_date = interim_analysis_time_span.start
    if (existing_end and existing_end > interim_start_date):
        interim_start_date = existing_end.astype('M8[D]').astype(datetime) + ONE_DAY
        interim_start_date = numpy.datetime64(interim_start_date)
    while interim_start_date <= interim_analysis_time_span.end:
        end_date = numpy.datetime64(f"{interim_start_date.astype('datetime64[Y]').astype(object).year}-12-31")
        load_span = Timespan(interim_start_date, min(end_date, interim_analysis_time_span.end))
        run_pipeline(pipeline, load_span, pipeline.loader.append, args, dataset_type="interim-reanalysis")
        interim_start_date = numpy.datetime64(f"{interim_start_date.astype('datetime64[Y]').astype(object).year + 1}-01-01")
    # Process the analysis data, starting one day after the end of the interim-analysis period
    # Convert the analysis_start_date calculation to handle the addition properly
    analysis_start_date = interim_analysis_time_span.end.astype('M8[D]').astype(datetime) + ONE_DAY
    analysis_start_date = numpy.datetime64(analysis_start_date)
    if (existing_end and existing_end > analysis_start_date):
        analysis_start_date = existing_end.astype('M8[D]').astype(datetime) + ONE_DAY
        analysis_start_date = numpy.datetime64(analysis_start_date)
    while analysis_start_date <= analysis_time_span.end:
        end_date = numpy.datetime64(f"{analysis_start_date.astype('datetime64[Y]').astype(object).year}-12-31")
        load_span = Timespan(analysis_start_date, min(end_date, analysis_time_span.end))
        run_pipeline(pipeline, load_span, pipeline.loader.append, args, dataset_type="analysis")
        analysis_start_date = numpy.datetime64(f"{analysis_start_date.astype('datetime64[Y]').astype(object).year + 1}-01-01")

    print("Checking if we ned to replace the data")

    # If a dataset exists, use the pipeline.loader.replace to update the data
    # Load the existing dataset again to get the new end date based on above
    existing_end = pipeline.assessor.get_end_date()
    # Retrieve the reanalysis and interim-analysis end dates from the existing dataset attributes
    reanalysis_end_date, interim_analysis_end_date = pipeline.assessor.get_analysis_end_dates()

    # Update reanalysis data if needed
    if reanalysis_end_date and reanalysis_end_date < reanalysis_time_span.end:
        # start_date = reanalysis_end_date + ONE_DAY
        # Convert the analysis_start_date calculation to handle the addition properly
        start_date = reanalysis_end_date.astype('M8[D]').astype(datetime) + ONE_DAY
        start_date = numpy.datetime64(start_date)
        while start_date <= reanalysis_time_span.end:
            end_date = numpy.datetime64(f"{start_date.astype('datetime64[Y]').astype(object).year}-12-31")
            load_span = Timespan(start_date, min(end_date, reanalysis_time_span.end))
            run_pipeline(pipeline, load_span, pipeline.loader.replace, args, dataset_type="reanalysis")
            start_date = numpy.datetime64(f"{start_date.astype('datetime64[Y]').astype(object).year + 1}-01-01")

    # Update interim-analysis data if needed
    if interim_analysis_end_date and interim_analysis_end_date < interim_analysis_time_span.end:
        # start_date = interim_analysis_end_date + ONE_DAY
        start_date = interim_analysis_end_date.astype('M8[D]').astype(datetime) + ONE_DAY
        start_date = numpy.datetime64(start_date)
        while start_date <= interim_analysis_time_span.end:
            end_date = numpy.datetime64(f"{start_date.astype('datetime64[Y]').astype(object).year}-12-31")
            load_span = Timespan(start_date, min(end_date, interim_analysis_time_span.end))
            run_pipeline(pipeline, load_span, pipeline.loader.replace, args, dataset_type="interim-reanalysis")
            start_date = numpy.datetime64(f"{start_date.astype('datetime64[Y]').astype(object).year + 1}-01-01")

    # Append new analysis data
    analysis_start_date = interim_analysis_time_span.end.astype('M8[D]').astype(datetime) + ONE_DAY
    analysis_start_date = numpy.datetime64(analysis_start_date)

    # If the existing end date is greater than the start of the analysis period, start from the existing end date
    # This should be the case if the analysis period is the only one that needs updating
    if (existing_end and existing_end < analysis_time_span.end):
        analysis_start_date = existing_end.astype('M8[D]').astype(datetime) + ONE_DAY
        analysis_start_date = numpy.datetime64(analysis_start_date)
    while analysis_start_date <= analysis_time_span.end:
        end_date = numpy.datetime64(f"{analysis_start_date.astype('datetime64[Y]').astype(object).year}-12-31")
        load_span = Timespan(analysis_start_date, min(end_date, analysis_time_span.end))
        run_pipeline(pipeline, load_span, pipeline.loader.append, args, dataset_type="analysis")
        analysis_start_date = numpy.datetime64(f"{analysis_start_date.astype('datetime64[Y]').astype(object).year + 1}-01-01")


def main(pipeline: Pipeline):
    args = _parse_args()
    try:
        cid = pipeline.loader.publisher.retrieve()
        # Get the remote timespan of the dataset
        remote_span = pipeline.assessor.get_remote_timespan()
        split_into_three_requests(pipeline, remote_span, cid, args)
        # if args["init"]:
        #     if cid and not args["--overwrite"]:
        #         raise docopt.DocoptExit(
        #             "Init would overwrite existing data. Use '--overwrite' flag if you really want to do this."
        #         )
        #     timedelta = _parse_timedelta(args["--timespan"])
        #     load_end = _add_delta(remote_span.start, timedelta - ONE_DAY)
        #     load_span = Timespan(remote_span.start, min(load_end, remote_span.end))
        #     run_pipeline(pipeline, load_span, pipeline.loader.initial, args, manual_override=True)
        #     return

        # elif args["append"]:
        #     if not cid:
        #         raise docopt.DocoptExit("Dataset has not been initialized.")
        #     # Get the timedelta which is like 4Y or 7Y
        #     timedelta = _parse_timedelta(args["--timespan"])
        #     # Get the existing dataset
        #     existing = pipeline.loader.dataset()
        #     # Get the last time value of the existing dataset
        #     existing_end = existing.time[-1].values
        #     # If the last time value of the existing dataset is greater than or equal to the last time value of the remote dataset
        #     # then there is no more data to load
        #     if existing_end >= remote_span.end:
        #         print("No more data to load.")
        #         return
        #     # Get the last time value of the existing dataset and add one day to it to get the start of the next load
        #     load_begin = _add_delta(existing_end, ONE_DAY)
        #     # Get the end of the next load
        #     load_end = _add_delta(load_begin, timedelta - ONE_DAY)
        #     # Get the timespan to load
        #     load_span = Timespan(load_begin, min(load_end, remote_span.end))
        #     run_pipeline(pipeline, load_span, pipeline.loader.append, args, manual_override=True)
        #     return

        # elif args["replace"]:
        #     load_span = _parse_timestamp(args["--daterange"])
        #     run_pipeline(pipeline, load_span, pipeline.loader.replace, args, manual_override=True)
        #     return

        # elif args["init-stepped"]:
        #     # Get the timescale to use (like 1 month, or any timedelta)
        #     timedelta = _parse_timedelta(args["--timespan"])          

        #     if not cid:
        #         # If the dataset does not exist, initialize the first timespan
        #         print("Dataset not found. Initializing dataset...")
        #         load_end = _add_delta(remote_span.start, timedelta - ONE_DAY)
        #         load_span = Timespan(remote_span.start, min(load_end, remote_span.end))
        #         run_pipeline(pipeline, load_span, pipeline.loader.initial, args, manual_override=True)
                
        #         # Update the CID to mark the dataset as initialized
        #         cid = pipeline.loader.dataset()

        #         load_begin = _add_delta(load_span.end, ONE_DAY)
                
        #     else:
        #         # If the dataset exists, pick up from where it left off
        #         print("Dataset exists. Resuming append process...")
        #         existing = pipeline.loader.dataset()
        #         existing_end = existing.time[-1].values
                
        #         # Check if there's more data to load
        #         if existing_end >= remote_span.end:
        #             print("No more data to load.")
        #             return
                
        #         # Calculate where to start appending from
        #         load_begin = _add_delta(existing_end, ONE_DAY)

        #     # Continue appending in steps until the remote dataset end is reached
        #     while load_begin < remote_span.end:
        #         load_end = _add_delta(load_begin, timedelta - ONE_DAY)
        #         load_span = Timespan(load_begin, min(load_end, remote_span.end))
        #         run_pipeline(pipeline, load_span, pipeline.loader.append, args, manual_override=True)
                
        #         # Update load_begin for the next step (next month)
        #         load_begin = _add_delta(load_end, ONE_DAY)
            
        #     return

        # # If nothing is specified, run the pipeline for the entire date range
        # run_pipeline(pipeline, None, pipeline.loader.replace, args, manual_override=False)

    except:
        if args["--pdb"]:
            pdb.post_mortem()
        raise



# def run_pipeline(pipeline, span, load, args):
#     print(
#         f"Loading {span.start.astype('<M8[s]').astype(object):%Y-%m-%d} "
#         f"to {span.end.astype('<M8[s]').astype(object):%Y-%m-%d}"
#     )
#     # If nothing exists, run the start for the entire date range
#     new_span, pipeline_info = pipeline.assessor.start(args=args)

#     sources = pipeline.fetcher.fetch(span)
#     extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
#     combined = pipeline.transformer(pipeline.combiner(extracted))
#     load(combined, span)




def run_pipeline(pipeline, span, load, args, manual_override=False, dataset_type=None):
    # split_into_three_requests(pipeline, span, load, args)
    # If nothing exists, run the start for the entire date range
    pipeline_info = pipeline.assessor.start(args=args)
    print(pipeline_info)

    # If there is a manual override, fetch, extract, transform, and load the data based on manual oveveride
    print(
        f"Loading {span.start.astype('<M8[s]').astype(object):%Y-%m-%d} "
        f"to {span.end.astype('<M8[s]').astype(object):%Y-%m-%d}", dataset_type, load
    )


    pipeline_info["dataset_to_download"] = dataset_type

    sources = pipeline.fetcher.fetch(span, pipeline_info)
    extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    print(extracted)
    combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
    load(combined, span)
    return
    
    # if new_span is None:
    #     return
    # existing_dataset = pipeline_info.get("existing_dataset", None)
    # start, end = new_span

    # # If there is a start and end date and there is no existing data, fetch, extract, transform, and load the data
    # # Any finalization data will be passed via the pipeline info
    # if (start and end and not existing_dataset):
    #     new_span = Timespan(start=start, end=end)
    #     loader = pipeline.loader.initial
    #     sources = pipeline.fetcher.fetch(new_span, pipeline_info)
    #     extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    #     combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
    #     loader(combined, new_span)
    #     return
        
    # # If something exists already append it first
    # if existing_dataset:
    #     append_span = Timespan(start=start, end=end)
    #     loader = pipeline.loader.append
    #     sources = pipeline.fetcher.fetch(append_span, pipeline_info)
    #     extracted = list(itertools.chain(*[pipeline.extractor(source) for source in sources]))
    #     combined, pipeline_info = pipeline.transformer(pipeline.combiner(extracted), pipeline_info)
    #     loader(combined, append_span)


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
        if s.endswith("D"):
            days = int(s[:-1])
            return relativedelta(days=days)
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