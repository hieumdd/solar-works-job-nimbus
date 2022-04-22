from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_date,
    transform_datetime,
    transform_float,
)


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/fdb233b1177440228227ada0671f6252?view=1",
    lambda rows: [
        {
            "number": row.get("Number"),
            "record_type": row.get("Record Type"),
            "status": row.get("Status"),
            "source": row.get("Source"),
            "created_by": row.get("Created By"),
            "sales_rep": row.get("Sales Rep"),
            "first_name": row.get("First Name"),
            "last_name": row.get("Last Name"),
            "date_status_change": transform_datetime(row.get("Date Status Change")),
            "city": row.get("City"),
            "total_install_cost": row.get("Total Install Cost"),
            "state": row.get("State"),
            "address_line": row.get("Address Line"),
            "main_phone": row.get("Main Phone"),
            "mobile_phone": row.get("Mobile Phone"),
            "id": row.get("Id"),
            "related": row.get("Related"),
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "LeadsOverall",
    [
        {"name": "number", "type": "STRING"},
        {"name": "record_type", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "created_by", "type": "STRING"},
        {"name": "sales_rep", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "date_status_change", "type": "TIMESTAMP"},
        {"name": "city", "type": "STRING"},
        {"name": "total_install_cost", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "address_line", "type": "STRING"},
        {"name": "main_phone", "type": "STRING"},
        {"name": "mobile_phone", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "related", "type": "STRING"},
    ],
    update=False,
)
