import os
import sys
import psycopg2
import requests
from datetime import datetime

log_file = "migration_output.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

POSTGRES_CONN = psycopg2.connect(
    host="13.127.174.112",
    port=5432,
    database="ims",
    user="imsadmin",
    password="Dghims!2025"
)
postgres_cursor = POSTGRES_CONN.cursor()

API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"

def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def process_documents(query, label, label_id):
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\nProcessing {label}:")
        print(regime, block, refid, sep='\n')

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
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

                pg_update = """
                    UPDATE dgh_staging.CMS_FILES
                    SET LOGICAL_DOC_ID = %s, LABEL_ID = %s
                    WHERE FILE_ID = %s
                """
                postgres_cursor.execute(pg_update, (logical_doc_id, label_id, file_id))
                POSTGRES_CONN.commit()
            else:
                print(f"⚠️ No docId found for {file_name} in responseObject")

        except Exception as upload_err:
            print(f"❌ Upload failed for {file_name}: {upload_err}")

try:
    print("✅ Connected to PostgreSQL database.")

    # === Queries ===
    query_upload_insurance_policy = """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."UPLOAD_INSURANCE_POLICY" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
    """

    query_perils_covered = """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."PERILS_COVERED" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
    """

    query_signature = """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."SIG_DIGITAL_SIG" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
    """

    query_additional_docs = """
        SELECT FAAO."REFID" ,CF.FILE_NAME ,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cfr.FILE_ID
        FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY faao 
        JOIN dgh_staging.CMS_FILE_REF cfr 
        ON CFR.REF_ID = FAAO."FILEREF"
        JOIN dgh_staging.CMS_FILES cf 
        ON CFR.FILE_ID = CF.FILE_ID
        WHERE CFR.IS_ACTIVE = '1'
    """

    process_documents(query_upload_insurance_policy, "Upload Insurance Policy", 32)
    process_documents(query_perils_covered, "Perils Covered", 31)
    process_documents(query_signature, "Signature/ Digital signature", 33)
    process_documents(query_additional_docs, "Additional File, If Any", 34)

    print("✅ All files processed.")

    postgres_cursor.close()
    POSTGRES_CONN.close()
    print("✅ PostgreSQL connection closed.")

except Exception as err:
    print(f"❌ Error occurred: {err}")

sys.stdout.close()
sys.stdout = sys.__stdout__