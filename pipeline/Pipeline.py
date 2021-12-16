from typing import Callable, Any

from libs.job_nimbus import get_driver, get_csv, get_data
from libs.bigquery import load
from libs.utils import compose

Pipeline = Callable[[Any], int]
DATASET = ""


def pipeline(
    report_url: str,
    transform_func: Callable[[list[dict]], list[dict]],
    table: str,
    schema: list[dict],
) -> Pipeline:
    return compose(
        load(DATASET, table, schema),
        transform_func,
        get_data,
        get_csv(report_url),
        get_driver,
    )


JobReportAllFields = pipeline(
    "https://app.jobnimbus.com/report/d749bfa04d2046b397d43f1a0dd6be69?view=1",
    lambda x: [{}],
    "JobReportAllField",
    [{}],
)

ContactReportAllFields = pipeline(
    "https://app.jobnimbus.com/report/843f91e32216487c807561d9e800f42f?view=1",
    lambda x: [{}],
    "ContactReportAllField",
    [{}],
)
