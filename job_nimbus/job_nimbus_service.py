from typing import Callable, Any, Union
from datetime import datetime
import uuid

from job_nimbus import (
    job_nimbus,
    job_nimbus_repo,
    job_report_all_fields,
    contact_reports_all_fields,
    customer_contact,
)

from db.bigquery import ID_KEY, TIME_KEY, load, id_schema
from utils.utils import compose

services = {
    i.table: i
    for i in [
        job_report_all_fields.pipeline,
        contact_reports_all_fields.pipeline,
        customer_contact.pipeline,
    ]
}


def _scrape_service(url: str) -> list[dict[str, Any]]:
    return compose(
        job_nimbus_repo.parse_content,
        job_nimbus_repo.get_data,
        job_nimbus_repo.get_csv(url),
    )


def _id_service(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **row,
            ID_KEY: uuid.uuid4(),
            TIME_KEY: datetime.utcnow().isoformat(timespec="seconds"),
        }
        for row in rows
    ]


def _load_service(
    pipeline: job_nimbus.Pipeline,
) -> Callable[[list[dict[str, Any]]], dict[str, Union[str, int]]]:
    return compose(
        lambda x: {"table": pipeline.table, "output_rows": x},
        load(pipeline.table, id_schema(pipeline.schema)),
        _id_service,
        pipeline.transform,
    )


def pipeline_service(pipeline: job_nimbus.Pipeline) -> dict[str, Union[str, int]]:
    return compose(
        _load_service(pipeline),
        _scrape_service(pipeline.url),
    )
