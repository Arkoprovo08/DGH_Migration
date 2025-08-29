import psycopg2
import json
from datetime import datetime

def parse_date(date_str):
    """Convert DD/MM/YYYY or DD-Mon-YYYY into YYYY-MM-DD format"""
    if not date_str or date_str.strip() == "":
        return None
    for fmt in ("%d/%m/%Y", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None  # if parsing fails

# === PostgreSQL Connection ===
conn = psycopg2.connect(
    host="13.127.174.112",
    port="5432",
    database="ims",
    user="imsadmin",
    password="Dghims!2025"
)
cursor = conn.cursor()

# === Fetch JSON data from source table ===
cursor.execute("""
    SELECT "REF_ID" , "SC_FORM_DATA_A" 
    FROM dgh_staging.form_self_certification
    WHERE "SC_FORM_DATA_A" IS NOT NULL
""")
rows = cursor.fetchall()

for ref_id, json_data in rows:
    try:
        data = json.loads(json_data)

        # Fix date fields
        data["CREATED_ON"] = parse_date(data.get("CREATED_ON"))
        data["APPLICATION_DATE"] = parse_date(data.get("APPLICATION_DATE"))

        cursor.execute("""
            INSERT INTO dgh_staging.temp_psc_extension_applications (
                ref_id,
                process_id,
                process_name,
                process_longname,
                status_id,
                created_by,
                created_on,
                form_ref,
                block_id,
                process_timeline,
                fileref,
                comments_id,
                block_type,
                bid_round,
                block_name,
                consortium,
                application_date,
                file_operating_committee_approval,
                file_valid_mining_lease,
                third_party_reserves_audit_report,
                balance_reserve,
                file_balance_reserve,
                file_technical_expertise,
                file_past_performance_1,
                file_past_performance_2,
                file_past_performance_3,
                fiscal_parameters_1,
                fiscal_parameters_2,
                fiscal_parameters_3,
                fiscal_parameters_4,
                certificate_from_contractor,
                revised_field_development_plan,
                file_ongoing_legal_cases,
                file_draft_amendment_psc,
                file_additional_documents,
                comments,
                declaration,
                name_authorised_signatory_contractor,
                designation
            )
            VALUES (
                %(REF_ID)s,
                %(PROCESS_ID)s,
                %(PROCESS_NAME)s,
                %(PROCESS_LONGNAME)s,
                %(STATUS_ID)s,
                %(CREATED_BY)s,
                %(CREATED_ON)s,
                %(FORM_REF)s,
                %(BLOCK_ID)s,
                %(PROCESS_TIMELINE)s,
                %(FILEREF)s,
                %(COMMENTS_ID)s,
                %(BLOCK_TYPE)s,
                %(BID_ROUND)s,
                %(BLOCK_NAME)s,
                %(CONSORTIUM)s,
                %(APPLICATION_DATE)s,
                %(FILE_OPERATING_COMMITTEE_APPROVAL)s,
                %(FILE_VALID_MINING_LEASE)s,
                %(THIRD_PARTY_RESERVES_AUDIT_REPORT)s,
                %(BALANCE_RESERVE)s,
                %(FILE_BALANCE_RESERVE)s,
                %(FILE_TECHNICAL_EXPERTISE)s,
                %(FILE_PAST_PERFORMANCE_1)s,
                %(FILE_PAST_PERFORMANCE_2)s,
                %(FILE_PAST_PERFORMANCE_3)s,
                %(FISCAL_PARAMETERS_1)s,
                %(FISCAL_PARAMETERS_2)s,
                %(FISCAL_PARAMETERS_3)s,
                %(FISCAL_PARAMETERS_4)s,
                %(CERTIFICATE_FROM_CONTRACTOR)s,
                %(REVISED_FIELD_DEVELOPMENT_PLAN)s,
                %(FILE_ONGOING_LEGAL_CASES)s,
                %(FILE_DRAFT_AMENDMENT_PSC)s,
                %(FILE_ADDITIONAL_DOCUMENTS)s,
                %(COMMENTS)s,
                %(DECLARATION)s,
                %(NAME_AUTHORISED_SIGNATORY_CONTRACTOR)s,
                %(DESIGNATION)s
            )
        """, data)

    except Exception as e:
        print(f"❌ Error inserting ref_id={ref_id}: {e}")
        conn.rollback()
    else:
        conn.commit()

cursor.close()
conn.close()
print("✅ Migration complete (dates parsed).")
