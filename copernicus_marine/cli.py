"""
ETL Pipeline for {name}

Usage:
    {script} [options] init
    {script} [options] append
    {script} interact

Options:
    -h --help         Show this screen.
    --timespan SPAN   How much data to load along the time axis. [default: 5Y]
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
            run_pipeline(pipeline, load_span, pipeline.loader.initial)

        elif args["append"]:
            if not cid:
                raise docopt.DocoptExit("Dataset has not been initialized.")

            timedelta = _parse_timedelta(args["--timespan"])
            remote_span = pipeline.fetcher.get_remote_timespan()
            existing = pipeline.loader.dataset()
            existing_end = existing.time[-1].values
            if existing_end >= remote_span.end:
                print("No more data to load.")
                return

            load_begin = _add_delta(existing_end, ONE_DAY)
            load_end = _add_delta(load_begin, timedelta - ONE_DAY)
            load_span = Timespan(load_begin, min(load_end, remote_span.end))
            run_pipeline(pipeline, load_span, pipeline.loader.append)

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
    except:  # noqa: E722
        pass

    raise docopt.DocoptExit(f"Unable to parse timespan: {s}")


def _add_delta(timestamp, delta):
    # Trying to manipulate datetimes with numpy gets pretty ridiculous
    timestamp = timestamp.astype("<M8[ms]").astype(datetime.datetime)
    timestamp = timestamp + delta

    # We only need to the day precision for these examples
    return numpy.datetime64(f"{timestamp.year}-{timestamp.month:02d}-{timestamp.day:02d}")