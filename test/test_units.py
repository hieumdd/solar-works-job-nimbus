from unittest.mock import Mock

import pytest

import csv

from main import main

from libs.job_nimbus import get_csv
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


@pytest.fixture(
    params=[
        "JOB REPORT ALL FIELDS.csv",
        "CONTACT REPORT ALL FIELDS.csv",
    ]
)
def data(request):
    with open(f"test/{request.param}") as f:
        reader = csv.DictReader(f)
        return [i for i in reader]


def test_get_csv(pipeline: Pipeline):
    assert get_csv(pipeline.url)(None)


def test_transform(data: list[dict], pipeline: Pipeline):
    assert pipeline.transform(data)


def test_load(data: list[dict], pipeline: Pipeline):
    assert (
        compose(load(DATASET, pipeline.table, pipeline.schema), pipeline.transform)(
            data
        )
        > 0
    )


@pytest.mark.parametrize(
    "table",
    [
        "JobReportAllFields",
        "ContactReportAllFields",
    ],
)
def test_pipeline(table):
    res = run({"table": table})
    res


def test_task():
    res = run(
        {
            "tasks": "hyros",
        }
    )
    assert res["tasks"] > 0
