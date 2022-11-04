from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_datetime,
    transform_float,
)


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/c26ab76397a8459da3f5edc1b41c045d?view=1",
    lambda rows: [
        {
            "sales_rep": row["Sales Rep"],
            "status": row["Status"],
            "total_install_cost": transform_float(row["Total Install Cost"]),
            "source": row["Source"],
            "stage": row["Stage"],
            "date_status_change": transform_datetime(row["Date Status Change"]),
            "date_created": transform_datetime(row["Date Created"]),
            "created_by": row["Created By"],
            "display": row["Display"],
            "state": row["State"],
            "id": row["Id"],
            "related": row["Related"],
            "contact_id": row["Contact Id"],
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "CompanyPresentationRan",
    [
        {"name": "sales_rep", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "total_install_cost", "type": "NUMERIC"},
        {"name": "source", "type": "STRING"},
        {"name": "stage", "type": "STRING"},
        {"name": "date_status_change", "type": "DATETIME"},
        {"name": "date_created", "type": "DATETIME"},
        {"name": "created_by", "type": "STRING"},
        {"name": "display", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
    ],
    update=False,
)
