from job_nimbus.job_nimbus import Pipeline
from utils.utils import transform_null, transform_date


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/e28afb3e19f24e899b7f71ce12a3124e?view=1",
    lambda rows: [
        {
            "name": row["Name"],
            "record_type": row["Record Type"],
            "status": row["Status"],
            "last_phone_call": transform_date(row["Last Phone Call"]),
            "count": row["Count"],
            "id": row["Id"],
            "related": row["Related"],
            "primary": row["Primary"],
            "task_id": row["Task Id"],
            "job_id": row["Job Id"],
            "contact_id": row["Contact Id"],
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "CustomerContact",
    [
        {"name": "name", "type": "STRING"},
        {"name": "record_type", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "last_phone_call", "type": "TIMESTAMP"},
        {"name": "count", "type": "NUMERIC"},
        {"name": "id", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "primary", "type": "STRING"},
        {"name": "task_id", "type": "STRING"},
        {"name": "job_id", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
    ],
    update=False,
)
