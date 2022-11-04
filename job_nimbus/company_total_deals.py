from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_datetime,
    transform_float,
)


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/48fb81373a0646578240af5e5f868f4c?view=1",
    lambda rows: [
        {
            "sales_rep": row["Sales Rep"],
            "source": row["Source"],
            "display": row["Display"],
            "record_type": row["Record Type"],
            "total_install_cost": transform_float(row["Total Install Cost"]),
            "system_size_kwdc": transform_float(row["System Size Kwdc"]),
            "city": row["City"],
            "date_status_change": transform_datetime(row["Date Status Change"]),
            "status": row["Status"],
            "state": row["State"],
            "roof_type": row["Roof Type"],
            "numberofmodules": transform_float(row["Numberofmodules"]),
            "panel_type": row["Panel Type"],
            "lender": row["Lender"],
            "interestrate": transform_float(row["Interestrate"]),
            "id": row["Id"],
            "related": row["Related"],
            "contact_id": row["Contact Id"],
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "CompanyTotalDeals",
    [
        {"name": "sales_rep", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "display", "type": "STRING"},
        {"name": "record_type", "type": "STRING"},
        {"name": "total_install_cost", "type": "NUMERIC"},
        {"name": "system_size_kwdc", "type": "NUMERIC"},
        {"name": "city", "type": "STRING"},
        {"name": "date_status_change", "type": "DATETIME"},
        {"name": "status", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "roof_type", "type": "STRING"},
        {"name": "numberofmodules", "type": "STRING"},
        {"name": "panel_type", "type": "STRING"},
        {"name": "lender", "type": "STRING"},
        {"name": "interestrate", "type": "NUMERIC"},
        {"name": "id", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
    ],
    update=False,
)
