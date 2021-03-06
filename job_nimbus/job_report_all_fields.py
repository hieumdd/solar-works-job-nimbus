from job_nimbus.job_nimbus import Pipeline
from utils.utils import (
    transform_null,
    transform_date,
    transform_datetime,
    transform_boolean,
    transform_float,
)

pipeline = Pipeline(
    "https://app.jobnimbus.com/report/d749bfa04d2046b397d43f1a0dd6be69?view=1",
    lambda rows: [
        {
            "name": row["Name"],
            "record_type": row["Record Type"],
            "status": row["Status"],
            "sales_rep": row["Sales Rep"],
            "primary": row["Primary"],
            "assigned_to": row["Assigned To"],
            "additional_permit_number": row["Additional Permit Number"],
            "address_line": row["Address Line"],
            "address_line2": row["Address Line2"],
            "approved_plans": transform_boolean(row["Approved Plans"]),
            "approved_estimates_total": transform_float(
                row["Approved Estimates (Total)"]
            ),
            "approved_invoices_balance_due": transform_float(
                row["Approved Invoices (Balance Due)"]
            ),
            "approved_invoices_total": transform_float(
                row["Approved Invoices (Total)"]
            ),
            "building_inspection_passed": transform_boolean(
                row["Building Inspection Passed"]
            ),
            "building_permit_approved": transform_boolean(
                row["Building Permit Approved"]
            ),
            "building_permit_id": row["Building Permit ID"],
            "city": row["City"],
            "company_cam_link": row["Company Cam Link"],
            "info": row["Info"],
            "country": row["Country"],
            "created_by": row["Created By"],
            "critter_guard_included": transform_boolean(row["Critter Guard Included"]),
            "customer_approved_design": transform_boolean(
                row["Customer Approved Design"]
            ),
            "date_created": transform_datetime(row["Date Created"]),
            "date_status_change": transform_datetime(row["Date Status Change"]),
            "days_in_status": transform_float(row["Days In Status"]),
            "date_updated": transform_datetime(row["Date Updated"]),
            "description": row["Description"],
            "disco_reco": row["Disco/Reco"],
            "disconnect_reconnect_date": transform_date(
                row["Disconnect/Reconnect Date"]
            ),
            "esr": row["ESR #"],
            "elec_inspection_scheduled": transform_date(
                row["Elec. Inspection Scheduled"]
            ),
            "electrical_inspection_passed": transform_boolean(
                row["Electrical Inspection Passed"]
            ),
            "electrical_permit": row["Electrical Permit"],
            "electrical_permit_id": row["Electrical Permit ID"],
            "end_date": transform_datetime(row["End Date"]),
            "funding_ready_for_permits": transform_boolean(
                row["Funding Ready For Permits"]
            ),
            "funding_ready_for_install": transform_boolean(
                row["Funding Ready For Install"]
            ),
            "hoa": transform_boolean(row["HOA"]),
            "hoa_approval": row["HOA Approval"],
            "icap_id": row["ICAP ID #"],
            "image_id": row["Image Id"],
            "inspecting_ahj": row["Inspecting AHJ"],
            "installation_date": transform_date(row["Installation Date"]),
            "interest_rate": transform_float(row["Interest Rate"]),
            "inverter_serial": row["Inverter Serial #"],
            "inverter_size": row["Inverter Size"],
            "is_archived": transform_boolean(row["Is Archived"]),
            "last_phone_call": transform_date(row["Last Phone Call"]),
            "source": row["Source"],
            "location": row["Location"],
            "lender": row["Lender"],
            "msp_location": row["MSP Location"],
            "material_ordered": transform_boolean(row["Material Ordered"]),
            "meter_set_requested": transform_boolean(row["Meter Set Requested"]),
            "most_recent_budget_gross_margin": transform_float(
                row["Most Recent Budget Gross Margin"]
            ),
            "most_recent_budget_gross_profit": transform_float(
                row["Most Recent Budget Gross Profit"]
            ),
            "most_recent_budget_revenue": transform_float(
                row["Most Recent Budget Revenue"]
            ),
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
            "notes_for_install": row["Notes For Install"],
            "number": row["Number"],
            "numberofmodules": row["Numberofmodules"],
            "panel_serial": row["Panel Serial #"],
            "panel_type": row["Panel Type"],
            "phone_number": row["Phone Number"],
            "project_manager": row["Project Manager"],
            "related": row["Related"],
            "requires_followup": row["Requires Follow-Up"],
            "roof_type": row["Roof Type"],
            "service_upgrade": transform_boolean(row["Service Upgrade"]),
            "site_assessment_date": transform_date(row["Site Assessment Date"]),
            "site_assessment_time": row["Site Assessment Time"],
            "stage": row["Stage"],
            "start_date": transform_datetime(row["Start Date"]),
            "state": row["State"],
            "storage": row["Storage"],
            "subcontractors": row["Subcontractors"],
            "is_qb_synced": row["Is Qb Synced"],
            "tags": row["Tags"],
            "count": transform_float(row["Count"]),
            "tech_review": transform_boolean(row["Tech Review"]),
            "trench": transform_boolean(row["Trench"]),
            "utility_company": row["Utility Company"],
            "zip": row["Zip"],
            "id": row["Id"],
            "task_id": row["Task Id"],
            "job_id": row["Job Id"],
            "contact_id": row["Contact Id"],
            "tier": row["Tier"],
        }
        for row in [
            {k.strip().replace(":", ""): transform_null(v) for k, v in row.items()}
            for row in rows
        ]
    ],
    "JobReportAllFields",
    [
        {"name": "name", "type": "STRING"},
        {"name": "record_type", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "sales_rep", "type": "STRING"},
        {"name": "primary", "type": "STRING"},
        {"name": "assigned_to", "type": "STRING"},
        {"name": "additional_permit_number", "type": "STRING"},
        {"name": "address_line", "type": "STRING"},
        {"name": "address_line2", "type": "STRING"},
        {"name": "approved_plans", "type": "BOOLEAN"},
        {"name": "approved_estimates_total", "type": "NUMERIC"},
        {"name": "approved_invoices_balance_due", "type": "NUMERIC"},
        {"name": "approved_invoices_total", "type": "NUMERIC"},
        {"name": "building_inspection_passed", "type": "BOOLEAN"},
        {"name": "building_permit_approved", "type": "BOOLEAN"},
        {"name": "building_permit_id", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "company_cam_link", "type": "STRING"},
        {"name": "info", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "created_by", "type": "STRING"},
        {"name": "critter_guard_included", "type": "BOOLEAN"},
        {"name": "customer_approved_design", "type": "BOOLEAN"},
        {"name": "date_created", "type": "TIMESTAMP"},
        {"name": "date_status_change", "type": "TIMESTAMP"},
        {"name": "days_in_status", "type": "NUMERIC"},
        {"name": "date_updated", "type": "TIMESTAMP"},
        {"name": "description", "type": "STRING"},
        {"name": "disco_reco", "type": "STRING"},
        {"name": "disconnect_reconnect_date", "type": "TIMESTAMP"},
        {"name": "esr", "type": "STRING"},
        {"name": "elec_inspection_scheduled", "type": "TIMESTAMP"},
        {"name": "electrical_inspection_passed", "type": "BOOLEAN"},
        {"name": "electrical_permit", "type": "STRING"},
        {"name": "electrical_permit_id", "type": "STRING"},
        {"name": "end_date", "type": "TIMESTAMP"},
        {"name": "funding_ready_for_permits", "type": "BOOLEAN"},
        {"name": "funding_ready_for_install", "type": "BOOLEAN"},
        {"name": "hoa", "type": "BOOLEAN"},
        {"name": "hoa_approval", "type": "STRING"},
        {"name": "icap_id", "type": "STRING"},
        {"name": "image_id", "type": "STRING"},
        {"name": "inspecting_ahj", "type": "STRING"},
        {"name": "installation_date", "type": "TIMESTAMP"},
        {"name": "interest_rate", "type": "NUMERIC"},
        {"name": "inverter_serial", "type": "STRING"},
        {"name": "inverter_size", "type": "STRING"},
        {"name": "is_archived", "type": "BOOLEAN"},
        {"name": "last_phone_call", "type": "TIMESTAMP"},
        {"name": "source", "type": "STRING"},
        {"name": "location", "type": "STRING"},
        {"name": "lender", "type": "STRING"},
        {"name": "msp_location", "type": "STRING"},
        {"name": "material_ordered", "type": "BOOLEAN"},
        {"name": "meter_set_requested", "type": "BOOLEAN"},
        {"name": "most_recent_budget_gross_margin", "type": "NUMERIC"},
        {"name": "most_recent_budget_gross_profit", "type": "NUMERIC"},
        {"name": "most_recent_budget_revenue", "type": "NUMERIC"},
        {"name": "most_recent_estimate_date_created", "type": "TIMESTAMP"},
        {"name": "most_recent_estimate_date_estimated", "type": "TIMESTAMP"},
        {"name": "most_recent_estimate_estimate_number", "type": "STRING"},
        {"name": "most_recent_estimate_total", "type": "NUMERIC"},
        {"name": "most_recent_invoice_date_created", "type": "TIMESTAMP"},
        {"name": "most_recent_invoice_total_date_invoice", "type": "TIMESTAMP"},
        {"name": "most_recent_invoice_invoice_number", "type": "STRING"},
        {"name": "most_recent_invoice_total", "type": "NUMERIC"},
        {"name": "notes_for_install", "type": "STRING"},
        {"name": "number", "type": "STRING"},
        {"name": "numberofmodules", "type": "NUMERIC"},
        {"name": "panel_serial", "type": "STRING"},
        {"name": "panel_type", "type": "STRING"},
        {"name": "phone_number", "type": "STRING"},
        {"name": "project_manager", "type": "STRING"},
        {"name": "related", "type": "STRING"},
        {"name": "requires_followup", "type": "STRING"},
        {"name": "roof_type", "type": "STRING"},
        {"name": "service_upgrade", "type": "BOOLEAN"},
        {"name": "site_assessment_date", "type": "TIMESTAMP"},
        {"name": "site_assessment_time", "type": "STRING"},
        {"name": "stage", "type": "STRING"},
        {"name": "start_date", "type": "TIMESTAMP"},
        {"name": "state", "type": "STRING"},
        {"name": "storage", "type": "STRING"},
        {"name": "subcontractors", "type": "STRING"},
        {"name": "is_qb_synced", "type": "BOOLEAN"},
        {"name": "tags", "type": "STRING"},
        {"name": "count", "type": "STRING"},
        {"name": "tech_review", "type": "BOOLEAN"},
        {"name": "trench", "type": "BOOLEAN"},
        {"name": "utility_company", "type": "STRING"},
        {"name": "zip", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "task_id", "type": "STRING"},
        {"name": "job_id", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
        {"name": "tier", "type": "STRING"},
    ],
    update=False,
)
