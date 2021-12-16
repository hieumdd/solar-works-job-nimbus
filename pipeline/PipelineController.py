import importlib

from pipeline.Pipeline import Pipeline

DATASET = ""


def factory(table: str) -> Pipeline:
    try:
        return getattr(importlib.import_module(f"pipeline.Pipeline"), table)
    except (ImportError, AttributeError, IndexError):
        raise ValueError(table)


def run(table: str) -> dict:
    return {
        "table": table,
        "results": factory(table)(None),
    }
