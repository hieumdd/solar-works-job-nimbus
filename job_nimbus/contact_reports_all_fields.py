from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_datetime,
    transform_boolean,
    transform_float,
)


pipeline = Pipeline(
    "https://app.jobnimbus.com/report/843f91e32216487c807561d9e800f42f?view=1",
    lambda rows: [
        {
            "display": row["Display"],
            "number": row["Number"],
            "record_type": row["Record Type"],
            "status": row["Status"],
            "source": row["Source"],
            "sales_rep": row["Sales Rep"],
            "created_by": row["Created By"],
            "state": row["State"],
            "assigned_to": row["Assigned To"],
            "stage": row["Stage"],
            "address_info": row["Address Info"],
            "address_line": row["Address Line"],
            "address_line2": row["Address Line2"],
            "approved_estimates_total": transform_float(
                row["Approved Estimates (Total)"]
            ),
            "approved_invoices_balance_due": transform_float(
                row["Approved Invoices (Balance Due)"]
            ),
            "approved_invoices_total": transform_float(
                row["Approved Invoices (Total)"]
            ),
            "city": row["City"],
            "company": row["Company"],
            "country": row["Country"],
            "date_created": transform_datetime(row["Date Created"]),
            "date_status_change": transform_datetime(row["Date Status Change"]),
            "days_in_status": transform_float(row["Days In Status"]),
            "date_updated": transform_datetime(row["Date Updated"]),
            "description": row["Description"],
            "down_payment": transform_float(row["Down Payment"]),
            "email": row["Email"],
            "end_date": transform_datetime(row["End Date"]),
            "fax_number": row["Fax Number"],
            "first_name": row["First Name"],
            "first_years_production": transform_float(row["Firstyearsproduction"]),
            "image_id": row["Image Id"],
            "interest_rate": row["Interestrate"],
            "inverter_size_kwac": row["Inverter Size Kwac"],
            "inverter_type": row["Inverter Type"],
            "is_archived": transform_boolean(row["Is Archived"]),
            "is_subcontractor": row["Is Sub Contractor"],
            "last_name": row["Last Name"],
            "lender": row["Lender"],
            "main_phone": row["Main Phone"],
            "mobile_phone": row["Mobile Phone"],
            "most_recent_estimate_date_created": transform_datetime(
                row["Most Recent Estimate (Date Created)"]
            ),
            "most_recent_estimate_date_estimated": transform_datetime(
                row["Most Recent Estimate (Date Estimated)"]
            ),
            "most_recent_estimate_estimate_number": row[
                "Most Recent Estimate (Estimate Number)"
            ],
            "most_recent_estimate_total": transform_float(
                row["Most Recent Estimate (Total)"]
            ),
            "most_recent_invoice_date_created": transform_datetime(
                row["Most Recent Invoice (Date Created)"]
            ),
            "most_recent_invoice_total_date_invoice": transform_datetime(
                row["Most Recent Invoice (Total) Date Invoice"]
            ),
            "most_recent_invoice_invoice_number": row[
                "Most Recent Invoice (Invoice Number)"
            ],
            "most_recent_invoice_total": transform_float(
                row["Most Recent Invoice (Total)"]
            ),
            "numberofmodules": transform_float(row["Numberofmodules"]),
            "panel_type": row["Panel Type"],
            "related": row["Related"],
            "roof_type": row["Roof Type"],
            "start_date": transform_datetime(row["Start Date"]),
            "storage": row["Storage"],
            "subcontractors": row["Subcontractors"],
            "is_qb_synced": row["Is Qb Synced"],
            "system_size_kwdc": transform_float(row["System Size Kwdc"]),
            "tags": row["Tags"],
            "count": transform_float(row["Count"]),
            "total_install_cost": transform_float(row["Total Install Cost"]),
            "website": row["Website"],
            "work_phone": row["Work Phone"],
            "years": transform_float(row["Years"]),
            "zip": row["Zip"],
            "id": row["Id"],
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items() if k}
            for row in rows
        ]
    ],
    "ContactReportAllFields",
    [
        {"name": "display", "type": "STRING"},
        {"name": "number", "type": "STRING"},
        {"name": "record_type", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "sales_rep", "type": "STRING"},
        {"name": "created_by", "type": "STRING"},
        {"name": "state", "type": "STRING"},
        {"name": "assigned_to", "type": "STRING"},
        {"name": "stage", "type": "STRING"},
        {"name": "address_info", "type": "STRING"},
        {"name": "address_line", "type": "STRING"},
        {"name": "address_line2", "type": "STRING"},
        {"name": "approved_estimates_total", "type": "NUMERIC"},
        {"name": "approved_invoices_balance_due", "type": "NUMERIC"},
        {"name": "approved_invoices_total", "type": "NUMERIC"},
        {"name": "city", "type": "STRING"},
        {"name": "company", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {"name": "date_status_change", "type": "TIMESTAMP"},
        {"name": "days_in_status", "type": "NUMERIC"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "description", "type": "STRING"},
        {"name": "down_payment", "type": "NUMERIC"},
        {"name": "email", "type": "STRING"},
        {"name": "end_date", "type": "TIMESTAMP"},
        {"name": "fax_number", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "first_years_production", "type": "NUMERIC"},
        {"name": "image_id", "type": "STRING"},
        {"name": "interest_rate", "type": "STRING"},
        {"name": "inverter_size_kwac", "type": "STRING"},
        {"name": "inverter_type", "type": "STRING"},
        {"name": "is_archived", "type": "BOOLEAN"},
        {"name": "is_subcontractor", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "lender", "type": "STRING"},
        {"name": "main_phone", "type": "STRING"},
        {"name": "mobile_phone", "type": "STRING"},
        {"name": "most_recent_estimate_date_created", "type": "TIMESTAMP"},
        {"name": "most_recent_estimate_date_estimated", "type": "TIMESTAMP"},
        {"name": "most_recent_estimate_estimate_number", "type": "STRING"},
        {"name": "most_recent_estimate_total", "type": "NUMERIC"},
        {"name": "most_recent_invoice_date_created", "type": "TIMESTAMP"},
        {"name": "most_recent_invoice_total_date_invoice", "type": "TIMESTAMP"},
        {"name": "most_recent_invoice_invoice_number", "type": "STRING"},
        {"name": "most_recent_invoice_total", "type": "NUMERIC"},
        {"name": "numberofmodules", "type": "NUMERIC"},
        {"name": "panel_type", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "roof_type", "type": "STRING"},
        {"name": "start_date", "type": "TIMESTAMP"},
        {"name": "storage", "type": "STRING"},
        {"name": "subcontractors", "type": "STRING"},
        {"name": "is_qb_synced", "type": "BOOLEAN"},
        {"name": "system_size_kwdc", "type": "NUMERIC"},
        {"name": "tags", "type": "STRING"},
        {"name": "count", "type": "NUMERIC"},
        {"name": "total_install_cost", "type": "NUMERIC"},
        {"name": "website", "type": "STRING"},
        {"name": "work_phone", "type": "STRING"},
        {"name": "years", "type": "NUMERIC"},
        {"name": "zip", "type": "STRING"},
        {"name": "id", "type": "STRING"},
    ],
)
