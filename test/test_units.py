from unittest.mock import Mock

import pytest

from main import main

from libs.job_nimbus import get_csv, parse_content
from libs.bigquery import load
from libs.utils import compose
from pipeline.Pipeline import Pipeline, JobReportAllFields, ContactReportAllFields
from pipeline.PipelineController import DATASET


def run(data: dict) -> dict:
    return main(Mock(get_json=Mock(return_value=data), args=data))


@pytest.fixture(
    params=[
        JobReportAllFields,
        ContactReportAllFields,
    ],
    ids=[
        JobReportAllFields.table,
        ContactReportAllFields.table,
    ],
)
def pipeline(request):
    return request.param


def read_data(file: str) -> bytes:
    with open(f"test/{file}.csv") as f:
        return f.read().encode("utf-8")


def test_get_csv(pipeline: Pipeline):
    assert get_csv(pipeline.url)(None)


def test_parse(pipeline: Pipeline):
    assert compose(
        parse_content,
        read_data,
    )(pipeline.table)


def test_transform(pipeline: Pipeline):
    assert compose(
        pipeline.transform,
        parse_content,
        read_data,
    )(pipeline.table)


def test_load_transform(pipeline: Pipeline):
    assert (
        compose(
            load(DATASET, pipeline.table, pipeline.schema),
            pipeline.transform,
            parse_content,
            read_data,
        )(pipeline.table)
        > 0
    )


def test_controller_pipeline(pipeline: Pipeline):
    assert run({"table": pipeline.table})


def test_controller_task():
    res = run({"task": "job-nimbus"})
    assert res["tasks"] > 0
