import os
import psycopg2
import requests
from datetime import datetime

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
FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"

# === Utility ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_status(document_name, status, doc_type_name, refid):
    postgres_cursor.execute("""
        INSERT INTO global_master.t_document_migration_status_details
        (document_name, document_migration_status, doc_type_name, refid)
        VALUES (%s, %s, %s, %s)
    """, (document_name, status, doc_type_name, refid))
    POSTGRES_CONN.commit()

def process_documents(query, label, label_id):
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\nProcessing {label}: {file_name}")

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            log_status(file_name, "File Not Found", label, refid)
            continue

        files = {'files': open(file_path, 'rb')}
        data = {
            'regime': regime,
            'block': block,
            'module': 'Financial Management',
            'process': 'Submission of Insurance and Indemnity',
            'financialYear': get_financial_year(created_on),
            'referenceNumber': refid,
            'label': label
        }

        try:
            response = requests.post(API_URL, files=files, data=data)
            response.raise_for_status()

            response_json = response.json()
            logical_doc_id = None
            for item in response_json.get("responseObject", []):
                if item.get("fileName") == file_name:
                    logical_doc_id = item.get("docId")
                    break

            if logical_doc_id:
                postgres_cursor.execute("""
                    UPDATE dgh_staging.CMS_FILES
                    SET LOGICAL_DOC_ID = %s, LABEL_ID = %s
                    WHERE FILE_ID = %s
                """, (logical_doc_id, label_id, file_id))
                POSTGRES_CONN.commit()

                log_status(file_name, "Uploaded", label, refid)
                print(f"✅ Uploaded: {file_name} ➜ docId: {logical_doc_id}")
            else:
                log_status(file_name, "Upload Failed", label, refid)
                print(f"⚠️ No docId found for {file_name}")

        except Exception as upload_err:
            log_status(file_name, "Upload Failed", label, refid)
            print(f"❌ Upload failed for {file_name}: {upload_err}")

# === Queries ===
queries = [
    ("""
        SELECT faao."REFID", cf.FILE_NAME, faao."BLOCKCATEGORY", faao."BLOCKNAME", faao."CREATED_ON", cf.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."UPLOAD_INSURANCE_POLICY" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
    """, "Upload Insurance Policy", 32),
    ("""
        SELECT faao."REFID", cf.FILE_NAME, faao."BLOCKCATEGORY", faao."BLOCKNAME", faao."CREATED_ON", cf.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."PERILS_COVERED" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
    """, "Perils Covered", 31),
    ("""
        SELECT faao."REFID", cf.FILE_NAME, faao."BLOCKCATEGORY", faao."BLOCKNAME", faao."CREATED_ON", cf.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."SIG_DIGITAL_SIG" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
    """, "Signature/ Digital signature", 33),
    ("""
        SELECT faao."REFID", cf.FILE_NAME, faao."BLOCKCATEGORY", faao."BLOCKNAME", faao."CREATED_ON", cfr.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao."FILEREF"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cfr.IS_ACTIVE = '1'
    """, "Additional File, If Any", 34)
]

# === Run all queries ===
for query, label, label_id in queries:
    process_documents(query, label, label_id)

# === Cleanup ===
postgres_cursor.close()
POSTGRES_CONN.close()
print("✅ All files processed and statuses logged.")