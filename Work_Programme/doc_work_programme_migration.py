import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect stdout to log file ===
log_file = "work_programme_attach_docs.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

# === PostgreSQL DB Config ===
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
FILES_DIR = r"C:\\Users\\Administrator.DGH\\Desktop\\dgh\\Files\\CMS\\Uploads"

# === Log migration status ===
def log_status(file_name, status, label, refid):
    insert_query = """
        INSERT INTO global_master.t_document_migration_status_details
        (document_name, document_migration_status, doc_type_name, refid)
        VALUES (%s, %s, %s, %s)
    """
    postgres_cursor.execute(insert_query, (file_name, status, label, refid))
    POSTGRES_CONN.commit()

# === Utility ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

# === Core document processing ===
def process_documents(query, label, label_id):
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)
        print(f"\n📄 File: {file_name}\nRefID: {refid} | Block: {block} | Regime: {regime}")

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            log_status(file_name, "File not found", label, refid)
            continue

        files = {'files': open(file_path, 'rb')}
        data = {
            'regime': regime,
            'block': block,
            'module': 'Operator Contracts and Agreements',
            'process': 'Well Location Review / Change / Deepening',
            'financialYear': get_financial_year(created_on),
            'referenceNumber': refid,
            'label': label
        }

        try:
            response = requests.post(API_URL, files=files, data=data)
            print(f"\n--- API response for {file_name} ---\n{response.text}\n")
            response.raise_for_status()

            response_json = response.json()
            response_objects = response_json.get("responseObject", [])

            logical_doc_id = None
            for item in response_objects:
                if item.get("fileName") == file_name:
                    logical_doc_id = item.get("docId")
                    break

            if logical_doc_id:
                print(f"✅ Uploaded: {file_name} ➜ docId: {logical_doc_id}")

                update_query = """
                    UPDATE dgh_staging.CMS_FILES
                    SET LOGICAL_DOC_ID = %s, LABEL_ID = %s
                    WHERE FILE_ID = %s
                """
                postgres_cursor.execute(update_query, (logical_doc_id, label_id, file_id))
                POSTGRES_CONN.commit()

                log_status(file_name, "Uploaded", label, refid)
            else:
                print(f"⚠️ No docId found in responseObject for {file_name}")
                log_status(file_name, "docId missing in response", label, refid)

        except Exception as upload_err:
            print(f"❌ Upload failed for {file_name}: {upload_err}")
            log_status(file_name, f"Upload failed: {str(upload_err)}", label, refid)

# === Queries and Labels SIT===
# queries = [
#     (
#         "Upload OCR", 200,
#         """
#         SELECT faao."WEL_LOC_CHA_DEEP_ID", cf.FILE_NAME, fwlcd.blockcategory , fwlcd.blockname , fwlcd.created_on , cf.FILE_ID
#         FROM dgh_staging.form_wel_loc_cha_de_data faao
#         JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."UPLOAD_OCR" = cmf.fileref
#         JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref
#         JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
#         JOIN dgh_staging.form_wel_loc_cha_deep fwlcd ON fwlcd.refid = faao."WEL_LOC_CHA_DEEP_ID"
#         WHERE cmf.ACTIVE = '1' AND fwlcd.status = '1' AND faao."ACTIVE" = '17'
#         """
#     ),
#     (
#         "Upload supporting document of approvals (Exploration/Appraisal/Development Plan)", 201,
#         """
#         SELECT faao."WEL_LOC_CHA_DEEP_ID", cf.FILE_NAME, fwlcd.blockcategory , fwlcd.blockname , fwlcd.created_on , cf.FILE_ID
#         FROM dgh_staging.form_wel_loc_cha_de_data faao
#         JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."WELL_TYPE_FILE" = cmf.fileref
#         JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref
#         JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
#         JOIN dgh_staging.form_wel_loc_cha_deep fwlcd ON fwlcd.refid = faao."WEL_LOC_CHA_DEEP_ID"
#         WHERE cmf.ACTIVE = '1' AND fwlcd.status = '1' AND faao."ACTIVE" = '17'
#         """
#     )
# ]

# DEV QUERIES
queries = [
    (
        "Upload Document", 236,
        """
        select
        	   a."REF_ID",
               cf.FILE_NAME,
               h.block_category,
               h.block_name,
               h.creation_date,
               cf.file_id
        FROM dgh_staging.form_work_programme_activities_be a
        JOIN upstream_data_management.t_work_programme_general_information  h 
            ON h.work_programme_application_number = a."REF_ID"
        JOIN dgh_staging.cms_file_ref cfr 
            ON cfr.ref_id = a.file_set_id 
        JOIN dgh_staging.cms_files cf 
            ON cf.file_id = cfr.file_id
        WHERE h.is_migrated = '1';
        """
    ),
    (
        "Upload Document", 236,
        """
        select
        	   a."REF_ID",
               cf.FILE_NAME,
               h.block_category,
               h.block_name,
               h.creation_date,
               cf.file_id
        FROM dgh_staging.form_work_programme_activities a
        JOIN upstream_data_management.t_work_programme_general_information  h 
            ON h.work_programme_application_number = a."REF_ID"
        JOIN dgh_staging.cms_file_ref cfr 
            ON cfr.ref_id = a.file_set_id 
        JOIN dgh_staging.cms_files cf 
            ON cf.file_id = cfr.file_id
        WHERE h.is_migrated = '1';
        """
    )
]

# === Run all queries ===
for label, label_id, query in queries:
    print(f"\n🔍 Starting processing for: {label}")
    process_documents(query, label, label_id)

# === Close DB connections ===
postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All files processed. PostgreSQL connection closed.")

# === Restore output stream ===
sys.stdout.close()
sys.stdout = sys.__stdout__
