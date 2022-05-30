from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_date,
    transform_datetime,
    transform_float,
)


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/b1828bca05704fda8266e1456cd7f679?view=1",
    lambda rows: [
        {
            "display": row.get("Display"),
            "date_status_change": transform_datetime(row.get("Date Status Change")),
            "status": row.get("Status"),
            "source": row.get("Source"),
            "sales_rep": row.get("Sales Rep"),
            "state": row.get("State"),
            "stage": row.get("Stage"),
            "address_info": row.get("Address Info"),
            "lender": row.get("Lender"),
            "interestrate": transform_float(row.get("Interestrate")),
            "years": transform_float(row.get("Years")),
            "total_install_cost": transform_float(row.get("Total Install Cost")),
            "numberofmodules": transform_float(row.get("Numberofmodules")),
            "location": row.get("Location"),
            "panel_type": row.get("Panel Type"),
            "id": row.get("Id"),
            "related": row.get("Related"),
            "contact_id": row.get("Contact Id"),
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "Sales",
    [
        {"name": "display", "type": "STRING"},
        {"name": "date_status_change", "type": "TIMESTAMP"},
        {"name": "status", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "sales_rep", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "stage", "type": "STRING"},
        {"name": "address_info", "type": "STRING"},
        {"name": "lender", "type": "STRING"},
        {"name": "interestrate", "type": "NUMERIC"},
        {"name": "years", "type": "NUMERIC"},
        {"name": "total_install_cost", "type": "NUMERIC"},
        {"name": "numberofmodules", "type": "NUMERIC"},
        {"name": "location", "type": "STRING"},
        {"name": "panel_type", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
    ],
    update=False,
)
