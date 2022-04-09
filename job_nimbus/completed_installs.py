from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_date,
    transform_datetime,
    transform_float,
)


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/281d586193ca499499a880f8badc8a45?view=1",
    lambda rows: [
        {
            "name": row.get("Name"),
            "location": row.get("Location"),
            "installation_date": transform_date(row.get("Installation Date")),
            "project_manager": row.get("Project Manager"),
            "site_assessment_date": transform_date(row.get("Site Assessment Date")),
            "panel_type": row.get("Panel Type"),
            "numberofmodules": transform_float(row.get("Numberofmodules")),
            "most_recent_budget_revenue": row.get("Most Recent Budget Revenue"),
            "status": row.get("Status"),
            "city": row.get("City"),
            "state": row.get("State"),
            "lender": row.get("Lender"),
            "record_type": row.get("Record Type"),
            "date_status_change": transform_datetime(row.get("Date Status Change")),
            "sales_rep": row.get("Sales Rep"),
            "number": row.get("Number"),
            "address_line": row.get("Address Line"),
            "address_line2": row.get("Address Line2"),
            "zip": row.get("Zip"),
            "id": row.get("Id"),
            "related": row.get("Related"),
            "primary": row.get("Primary"),
            "task_id": row.get("Task Id"),
            "job_id": row.get("Job Id"),
            "contact_id": row.get("Contact Id"),
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "CompletedInstalls",
    [
        {"name": "name", "type": "STRING"},
        {"name": "location", "type": "STRING"},
        {"name": "installation_date", "type": "TIMESTAMP"},
        {"name": "project_manager", "type": "STRING"},
        {"name": "site_assessment_date", "type": "TIMESTAMP"},
        {"name": "panel_type", "type": "STRING"},
        {"name": "numberofmodules", "type": "NUMERIC"},
        {"name": "most_recent_budget_revenue", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "lender", "type": "STRING"},
        {"name": "record_type", "type": "STRING"},
        {"name": "date_status_change", "type": "TIMESTAMP"},
        {"name": "sales_rep", "type": "STRING"},
        {"name": "number", "type": "STRING"},
        {"name": "address_line", "type": "STRING"},
        {"name": "address_line2", "type": "STRING"},
        {"name": "zip", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "primary", "type": "STRING"},
        {"name": "task_id", "type": "STRING"},
        {"name": "job_id", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
    ],
    update=False,
)
