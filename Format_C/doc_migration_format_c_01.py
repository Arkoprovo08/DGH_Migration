import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect stdout to log file ===
log_file = "format_c_migration_output.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

# === PostgreSQL DB Config ===
POSTGRES_CONN = psycopg2.connect(
    host="3.110.185.154",
    port=5432,
    database="ims",
    user="postgres",
    password="P0$tgres@dgh"
)
postgres_cursor = POSTGRES_CONN.cursor()

# === API Config ===
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"

# === Utility ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def process_documents(query, label, label_id):
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\nProcessing {label}:")
        print(regime, block, refid, sep=' | ')

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue

        files = {'files': open(file_path, 'rb')}
        data = {
            'regime': regime,
            'block': block,
            'module': 'Operator Contracts and Agreements',
            'process': 'Format-C: Commercial Discovery (Declaration of Commerciality)',
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

# === Queries and Labels ===
queries = [
    (
        "OCR Available", 43,
        """
        SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cf.FILE_ID
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.upload_ocr_yes = cmf.fileref 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """
    ),
    (
        "Justification for OCR Unavailability", 490,
        """
        SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cf.FILE_ID
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.upload_ocr_no = cmf.fileref 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """
    ),
    (
        "No of wells drilled and status", 45,
        """
        SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cf.FILE_ID
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.no_of_wells_drilled = cmf.fileref 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """
    ),
    (
        "Areal extent of the pool/field", 57,
        """
        SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cf.FILE_ID
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.areal_extent = cmf.fileref 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """
    ),
    (
        "Likely facility to be deployed", 58,
        """
        SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cf.FILE_ID
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.fac_deploy = cmf.fileref 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """
    ),
    (
        "Results of techno-economic analysis", 59,
        """
        SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cf.FILE_ID
        FROM dgh_staging.form_commerical_dis_format_c faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.tec_eco = cmf.fileref 
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.fileref 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """
    ),
    (
    "Upload Additional Documents", 106,
    """
    SELECT faao.REFID, cf.FILE_NAME, faao.BLOCKCATEGORY, faao.BLOCKNAME, faao.CREATED_ON, cfr.FILE_ID
    FROM dgh_staging.form_commerical_dis_format_c faao 
    JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao.FILEREF
    JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
    WHERE cfr.IS_ACTIVE = 1
    """
    )
]

# === Run all queries ===
for label, label_id, query in queries:
    process_documents(query, label, label_id)

# === Close connections ===
postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All files processed. PostgreSQL connection closed.")

# Restore output stream
sys.stdout.close()
sys.stdout = sys.__stdout__
