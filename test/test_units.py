from unittest.mock import Mock

import pytest

from main import main

from libs.job_nimbus import get_csv


def run(data: dict) -> dict:
    return main(Mock(get_json=Mock(return_value=data), args=data))


@pytest.fixture(
    params=[
        "https://app.jobnimbus.com/report/d749bfa04d2046b397d43f1a0dd6be69?view=1",
        "https://app.jobnimbus.com/report/843f91e32216487c807561d9e800f42f?view=1",
    ]
)
def report_url(request):
    return request.param


def test_get_csv(report_url):
    assert get_csv(report_url)(None)


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
