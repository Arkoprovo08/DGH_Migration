import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect output/log ===
sys.stdout = open("eoy_audited_accounts_doc_migration_log.txt", "w", encoding="utf-8")
sys.stderr = sys.stdout

# === PostgreSQL Connection ===
POSTGRES_CONN = psycopg2.connect(
    host="13.127.174.112",
    port=5432,
    database="ims",
    user="imsadmin",
    password="Dghims!2025"
)
postgres_cursor = POSTGRES_CONN.cursor()

# === API Config ===
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"

# === Btn to Doc Type Mapping ===
BTN_MAP = {
    "Btn_addition_file": ("Upload Additional Documents", 468),
    "Btn_all_Exploration": ("Quantum of all Exploration G&G activities (Estimate vs Actual)", 454),
    "Btn_all_Exploration1": ("Quantum of all Exploration G&G activities (Estimate vs Actual)", 454),
    "Btn_annual_expenditure": ("Whether the Annual Expenditure is within the MC approved Budget or Appendix H (whichever less), if any (If Yes, Add/Remove Files)", 476),
    "Btn_Annual_Expenditure": ("Whether the Annual Expenditure is within the MC approved Budget or Appendix H (whichever less), if any (If Yes, Add/Remove Files)", 476),
    "Btn_Annual_Expenditure1": ("Whether the Annual Expenditure is within the MC approved Budget or Appendix H (whichever less), if any (If Yes, Add/Remove Files)", 476),
    "Btn_Annual_Expenditure_Budget": ("Whether the Annual Expenditure is within the MC approved Budget or Appendix H (whichever less), if any (If Yes, Add/Remove Files)", 476),
    "Btn_Annual_Expenditure_is": ("Whether the Annual Expenditure is within the MC approved Budget or Appendix H (whichever less), if any (If Yes, Add/Remove Files)", 476),
    "Btn_Annual_Expenditure_mc": ("Whether the Annual Expenditure is within the MC approved Budget or Appendix H (whichever less), if any (If Yes, Add/Remove Files)", 476),
    "Btn_Are_Sale_Purchase": ("Does Sale & Purchase of Goods are in compliance to PSC? (If Yes, Upload)", 433),
    "Btn_Audited_Accounts": ("Audited Accounts Statement and end of year statement submitted as per DGH Format (If Yes, Upload)", 411),
    "Btn_Audited_Accounts1": ("Audited Accounts Statement and end of year statement submitted as per DGH Format (If Yes, Upload)", 411),
    "Btn_Budget_Estimate": ("MC approved Budget submitted (If Yes, Upload)", 444),
    "Btn_Budget_Estimate1": ("MC approved Budget submitted (If Yes, Upload)", 444),
    "Btn_Compliance_PSC": ("Compliance of PSC terms/GOI Notifications/Statutory compliances (If Yes, Upload)", 412),
    "Btn_Compliance_PSC1": ("Compliance of PSC terms/GOI Notifications/Statutory compliances (If Yes, Upload)", 412),
    "Btn_Core_studies": ("Core studies", 413),
    "Btn_Detailed_break_up": ("Detailed break-up of the annual expenditure in line with the Appendix-H estimates and details of excess ...", 414),
    "Btn_Detailed_break_up1": ("Detailed break-up of the annual expenditure in line with the Appendix-H estimates and details of excess ...", 414),
    "Btn_Details_sales1": ("Details of sales realisation - (if any) i.e. Crude/Gas/condensate invoice wise on monthly and quarterly ...", 419),
    "Btn_Details_sales_realisation": ("Details of sales realisation - (if any) i.e. Crude/Gas/condensate invoice wise on monthly and quarterly ...", 419),
    "Btn_development_line": ('Does elements of cost in Audited accounts for "development" in line with WP&B template for category B', 427),
    "Btn_Did_MC_approv": ("Did MC approved last year Audited Accounts and End of Year statement (If Yes, Upload)", 421),
    "Btn_Did_MC_approv1": ("Did MC approved last year Audited Accounts and End of Year statement (If Yes, Upload)", 421),
    "Btn_DOC_FDP_FDP1": ("DOC/FDP/FDP Rev No. (Latest) /Reservoir engineering reports", 422),
    "Btn_Does_all_outstanding": ("Does all outstanding dues notified by GOI/DGH paid? (If Yes, Upload)", 423),
    "Btn_Does_all_outstanding1": ("Does all outstanding dues notified by GOI/DGH paid? (If Yes, Upload)", 423),
    "Btn_Does_Audited": ("Does Audited Accounts and End of Year statement contains all necessary disclaimer? (If yes, Upload)", 424),
    "Btn_Does_Audited1": ("Does Audited Accounts and End of Year statement contains all necessary disclaimer? (If yes, Upload)", 424),
    "Btn_Does_Cost_Recoverable": ("Does Cost Recoverable are as per PSC Terms &Condition? (If Yes, Upload)", 426),
    "Btn_Does_Cost_Recoverable1": ("Does Cost Recoverable are as per PSC Terms &Condition? (If Yes, Upload)", 426),
    "Btn_Does_GOI1": ("Does GOI PP are as per PSC Terms &Condition? (If Yes, Upload)", 431),
    "Btn_Does_Revenue_computed1": ("Does Revenue computed as per PSC Terms &Condition? (If Yes, Upload)", 432),
    "Btn_Does_Sale": ("Does Sale & Purchase of Goods are in compliance to PSC? (If Yes, Upload)", 433),
    "Btn_Does_Sale1": ("Does Sale & Purchase of Goods are in compliance to PSC? (If Yes, Upload)", 433),
    "Btn_drilling_part": ("In case of drilling of part well or spill over well( from previous year); the meterage drilled / TD meterage ...", 439),
    "Btn_EoY_Statement": ("EoY Statement as per DGH Format with Schedule and notes to account", 554),
    "Btn_Foreign_Exchange": ("Details of Foreign Exchange Gain or Loss in USD terms, if any", 415),
    "Btn_Foreign_Exchange1": ("Details of Foreign Exchange transactions and the value in USD in line with PSC", 416),
    "Btn_Format_3B": ("Format 3B, complete with all details, ...", 436),
    "Btn_Format_3B1": ("Format 3B, complete with all details, ...", 436),
    "Btn_GOI": ("Does GOI PP are as per PSC Terms &Condition? (If Yes, Upload)", 431),
    "Btn_Investment_multiple": ("Investment multiple calculation, detailed workings of Notional Tax calculation ...", 440),
    "Btn_Investment_multiple_calculation1": ("Investment multiple calculation, detailed workings of Notional Tax calculation ...", 440),
    "Btn_Justifications": ("JUSTIFICATIONS OF ALL EXPLORATION/APPRAISAL G&G ACTIVITIES AMOUNTING TO THE VARIANCE", 442),
    "Btn_Justifications1": ("JUSTIFICATIONS OF ALL EXPLORATION/APPRAISAL G&G ACTIVITIES AMOUNTING TO THE VARIANCE", 442),
    "Btn_last_year_Audited": ("Is last year Audited Account MC approved? (If Yes, PLEASE UPLOAD THE LAST YEAR MCR*)", 441),
    "Btn_MC_approved_Appointment": ("MC approved Appointment of Auditor submitted (If Yes, Upload)", 443),
    "Btn_MC_approved_Appointment1": ("MC approved Appointment of Auditor submitted (If Yes, Upload)", 443),
    "Btn_MC_approved_Appointment_MCR": ("MC approved Appointment of Auditor submitted (If Yes, Upload)", 443),
    "Btn_MC_approved_Budget": ("MC approved Budget with MCR (if Yes, upload file)", 445),
    "Btn_MC_approved_Budget1": ("MC approved Budget with MCR (if Yes, upload file)", 445),
    "Btn_MC_approved_Budget_MCR": ("MC approved Budget with MCR (if Yes, upload file)", 445),
    "Btn_OC_approved": ("Does Audited Accounts and end of year statement OC approved (If Yes, Upload)", 425),
    "Btn_OC_approved1": ("Does Audited Accounts and end of year statement OC approved (If Yes, Upload)", 425),
    "Btn_operation_cost": ("MDT, DST, Well testing, PVT Sampling, PLT etc.- operation cost", 446),
    "Btn_operation_cost1": ("MDT, DST, Well testing, PVT Sampling, PLT etc.- operation cost", 446),
    "Btn_party_transactions1": ("Details of related party transactions(if any) and certificate as per Audit and PSC", 418),
    "Btn_Production_operation": ("Does elements of cost in Audited accounts for Production operation in line with WP&B template for category B", 428),
    "Btn_Production_Services": ("Reservoir Engineering Training, Seminar and Workshop", 460),
    "Btn_Profit_Petroleum": ("Royalty & Cess statement paid during the year submitted (If Yes, Upload)", 464),
    "Btn_PVT_Analysis": ("PVT Analysis", 453),
    "Btn_reconciliation": ("Details of reconciliation of closing/opening stock (if any) and their valuation in accounts", 417),
    "Btn_reconciliation1": ("Detals of reconciliation of closing/ opening stock (if any) and their valuation in accounts", 420),
    "Btn_Reference_location": ("Reference location/ area/ field/ reservoir/ well for all Exploration/ Appraisal G&G activities (Estimate vs Actual) on a scaled map with all drilled well(s), if any", 457),
    "Btn_Reference_location1": ("Reference location/ area/ field/ reservoir/ well for all Exploration/ Appraisal G&G activities (Estimate vs Actual) on a scaled map with all drilled well(s), if any", 457),
    "Btn_Reservoir_engineering1": ("Reservoir engineering reports", 458),
    "Btn_Revenue_compution": ("Revenue computation as per PSC Terms and Condition", 463),
    "Btn_Revised_Estimate": ("Reference Exploration/Appraisal Programme and its MC-approval i) MC-approved Revised Estimate", 455),
    "Btn_Revised_Estimate1": ("Reference Exploration/Appraisal Programme and its MC-approval ii) MC-approved Budget Estimate", 456),
    "Btn_Royalty_Cess1": ("Royalty & Cess statement paid during the year submitted (If Yes, Upload)", 464),
    "Btn_SRF_created": ("SRF created as per SRF'99 & SRG'2018 created", 466),
    "Btn_SRF_created1": ("SRF created as per SRF'99 & SRG'2018 created", 466),
    "Btn_Stock_statement": ("Upload Stock statement for FY( for Crude oil, Condensate, ANG,NG,Internal consumption)", 472),
    "Btn_Surface_facilities": ("Does elements of cost in Audited accounts for Surface facilities in line with WP&B template for category B", 430),
    "Btn_table_showing": ("Whether a table showing quantum of drilling activities carried out vis-a-vis drilling activities approved in WP&B of the corresponding FY has been included", 475),
    "Btn_table_showing1": ("Whether a table showing quantum of drilling activities carried out vis-a-vis drilling activities approved in WP&B of the corresponding FY has been included", 475),
    "Btn_transactions": ("Finance Details of related party transactions(if any) and certificate as per Audit and PSC", 560),
    "Btn_transient_interpretation": ("Pressure transient interpretation, analysis & reports", 452),
    "Btn_Variance_Analysis": ("Variance Analysis of Actual V/s Budgeted amount submitted", 473),
    "Btn_Variance_Analysis1": ("Variance Analysis of Actual V/s Budgeted amount submitted", 473),
    "Btn_Variance_Analysis_Actual": ("Variance Analysis of Actual V/s Budgeted amount submitted", 473),
    "Btn_Variance_Statement": ("VARIANCE STATEMENT ALONG WITH REASONS/ JUSTIFICATION FOR SAME SHALL BE INCLUDED FOR VARIANCE W.R.T. WP&B", 474),
    "Btn_Variance_Statement1": ("VARIANCE STATEMENT ALONG WITH REASONS/ JUSTIFICATION FOR SAME SHALL BE INCLUDED FOR VARIANCE W.R.T. WP&B", 474),
}

def get_financial_year(created_on):
    if isinstance(created_on, str):
        try:
            created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
        except Exception:
            created_on = datetime.strptime(created_on, "%Y-%m-%d")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_migration_status(document_name, status, doc_type_name, refid):
    postgres_cursor.execute(
        """
        INSERT INTO global_master.t_document_migration_status_details 
            (document_name, document_migration_status, doc_type_name, refid)
        VALUES (%s, %s, %s, %s)
        """,
        (document_name, status, doc_type_name, refid)
    )
    POSTGRES_CONN.commit()

print("=== Starting EOY Document Migration ===")

query = """
SELECT
    fad."REFID",
    fad."DATA_ID",
    cf.file_name,
    faad."BLOCKCATEGORY",
    faad."BLOCKNAME",
    faad."CREATED_ON",
    cf.file_id
FROM dgh_staging.form_audited_accounts_data fad
LEFT JOIN dgh_staging.form_audited_accounts faad
    ON fad."REFID" = faad."REF_ID"
LEFT JOIN dgh_staging.cms_file_ref cfr
    ON fad."DATA_VALUE" = cfr.ref_id
    AND cfr.is_active = 1
LEFT JOIN dgh_staging.cms_files cf
    ON cfr.file_id = cf.file_id
    AND cf.is_active = 1
LEFT JOIN dgh_staging.frm_workitem_master_new a
    ON a.ref_id = fad."REFID"
WHERE
    fad."DATA_ID" LIKE 'Btn%%'
    AND fad."STATUS" = 1
    AND cfr.file_id IS NOT NULL
ORDER BY fad."REFID";
"""

postgres_cursor.execute(query)
rows = postgres_cursor.fetchall()

for row in rows:
    refid, data_id, file_name, block_category, block_name, created_on, file_id = row
    btn_type = BTN_MAP.get(data_id)
    if not btn_type:
        print(f"⚠️ DATA_ID {data_id} not mapped to any document type. Skipping file: {file_name}")
        log_migration_status(file_name, "Upload Failed", None, refid)
        continue
    doc_type_name, doc_type_id = btn_type

    file_path = os.path.join(FILES_DIR, file_name)
    print(f"\n📄 Uploading File: {file_name}\nRefID: {refid}\nBlock: {block_name}\nDATA_ID: {data_id}\nType: {doc_type_name} ({doc_type_id})")

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        log_migration_status(file_name, "File Not Found", doc_type_name, refid)
        continue

    try:
        with open(file_path, 'rb') as f:
            files = {'files': f}
            data = {
                'blockCategory': block_category,
                'block': block_name,
                'module': 'Financial Management',
                'process': 'EOY Audited Accounts',
                'financialYear': get_financial_year(created_on),
                'referenceNumber': refid,
                'label': doc_type_name,
                'documentTypeId': doc_type_id
            }
            response = requests.post(API_URL, files=files, data=data)
            print(f"🌐 API Response: {response.text}")
            response.raise_for_status()
            response_json = response.json()
            response_objects = response_json.get("responseObject", [])

            logical_doc_id = None
            for obj in response_objects:
                if obj.get("fileName") == file_name:
                    logical_doc_id = obj.get("docId")
                    break

            if logical_doc_id:
                print(f"✅ Uploaded. docId: {logical_doc_id}")
                update_query = """
                    UPDATE dgh_staging.cms_files
                    SET logical_doc_id = %s, label_id = %s
                    WHERE file_id = %s
                """
                postgres_cursor.execute(update_query, (logical_doc_id, doc_type_id, file_id))
                POSTGRES_CONN.commit()
                log_migration_status(file_name, "Uploaded", doc_type_name, refid)
            else:
                print(f"⚠️ docId not found in API response.")
                log_migration_status(file_name, "Upload Failed", doc_type_name, refid)
    except Exception as err:
        print(f"❌ Upload failed for {file_name}: {err}")
        log_migration_status(file_name, "Upload Failed", doc_type_name, refid)

postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All Btn files processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__
