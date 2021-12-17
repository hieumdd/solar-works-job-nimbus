from typing import Callable, Any
import importlib

from libs.job_nimbus import get_csv, get_data
from libs.bigquery import load
from libs.utils import compose
from pipeline.Pipeline import Pipeline

DATASET = "JobNimbus"


def pipeline(pipeline: Pipeline) -> Callable[[Any], int]:
    return compose(
        load(DATASET, pipeline.table, pipeline.schema),
        pipeline.transform,
        get_data,
        get_csv(pipeline.url),
    )


def factory(table: str) -> Pipeline:
    try:
        return getattr(importlib.import_module(f"pipeline.Pipeline"), table)
    except (ImportError, AttributeError, IndexError):
        raise ValueError(table)


def run(table: str) -> dict:
    return {
        "table": table,
        "results": pipeline(factory(table))(None),
    }
