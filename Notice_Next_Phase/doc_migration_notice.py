import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect stdout to log file ===
log_file = "notice_next_phase_output.txt"
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
FILES_DIR = r"\\192.168.0.126\it\CMS\Uploads1"

# === Utility Functions ===

def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_migration_status(file_name, status, doc_type_name, refid):
    try:
        insert_query = """
            INSERT INTO global_master.t_document_migration_status_details (
                document_name, document_migration_status, doc_type_name, refid
            ) VALUES (%s, %s, %s, %s)
        """
        postgres_cursor.execute(insert_query, (file_name, status, doc_type_name, refid))
        POSTGRES_CONN.commit()
    except Exception as e:
        print(f"⚠️ Failed to log migration status for {file_name}: {e}")

def process_documents(query, label, label_id):
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\n📄 Processing: {label}")
        print(f"RefID: {refid} | File: {file_name} | Block: {block}")

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            log_migration_status(file_name, "File Not Found", label, refid)
            continue

        try:
            with open(file_path, 'rb') as f:
                files = {'files': f}
                data = {
                    'regime': regime,
                    'block': block,
                    'module': 'Upstream Data Management',
                    'process': 'Notice for Next Phase of Relinquishment',
                    'financialYear': get_financial_year(created_on),
                    'referenceNumber': refid,
                    'label': label
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
                        UPDATE dgh_staging.CMS_FILES
                        SET LOGICAL_DOC_ID = %s, LABEL_ID = %s
                        WHERE FILE_ID = %s
                    """
                    postgres_cursor.execute(update_query, (logical_doc_id, label_id, file_id))
                    POSTGRES_CONN.commit()
                    log_migration_status(file_name, "Uploaded", label, refid)
                else:
                    print(f"⚠️ docId not found in API response.")
                    log_migration_status(file_name, "Upload Failed", label, refid)

        except Exception as err:
            print(f"❌ Upload failed for {file_name}: {err}")
            log_migration_status(file_name, "Upload Failed", label, refid)

# === Queries and Labels ===
# SIT
# queries = [
#     (
#         """
#         SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
#         FROM dgh_staging.form_notice_next_phase_relin faao
#         JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."AMT_BG_LD_COUMP" = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
#         WHERE cmf.ACTIVE = '1'
#         """,
#         "Amount of BG/LD/CoUMP submitted (Upload BG/Transaction Receipt)",
#         38
#     ),
#     (
#         """
#         SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
#         FROM dgh_staging.form_notice_next_phase_relin faao
#         JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."MWP_CEP" = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
#         WHERE cmf.ACTIVE = '1'
#         """,
#         "Status report to be uploaded regarding activities completed w.r.t Minimum Work Programme (MWP) and Mandatory Work Programme",
#         37
#     ),
#     (
#         """
#         SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
#         FROM dgh_staging.form_notice_next_phase_relin faao
#         JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."CONTRACT_AREA" = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
#         WHERE cmf.ACTIVE = '1'
#         """,
#         "Relinquishment of Entire Contract area or part area (Upload Map of area being relinquished with co-ordinates/shape file to be uploaded)",
#         36
#     ),
#     (
#         """
#         SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
#         FROM dgh_staging.form_notice_next_phase_relin faao
#         JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."SIG_DIGITAL_SIG" = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
#         JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
#         WHERE cmf.ACTIVE = '1'
#         """,
#         "Signature / Digital Signature",
#         491
#     ),
#     (
#         """
#         SELECT FAAO."REFID" ,CF.FILE_NAME ,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cfr.FILE_ID
#         FROM dgh_staging.form_notice_next_phase_relin faao 
#         JOIN dgh_staging.CMS_FILE_REF cfr 
#         ON CFR.REF_ID = FAAO."FILEREF"
#         JOIN dgh_staging.CMS_FILES cf 
#         ON CFR.FILE_ID = CF.FILE_ID
#         WHERE CFR.IS_ACTIVE = '1'
#         """,
#         "Additional File, If Any",
#         39
#     )
# ]
#  DEV
queries = [
    (
        """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.form_notice_next_phase_relin faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."AMT_BG_LD_COUMP" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """,
        "Amount of BG/LD/CoUMP submitted (Upload BG/Transaction Receipt)",
        38
    ),
    (
        """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.form_notice_next_phase_relin faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."MWP_CEP" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """,
        "Status report to be uploaded regarding activities completed w.r.t Minimum Work Programme (MWP) and Mandatory Work Programme",
        37
    ),
    (
        """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.form_notice_next_phase_relin faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."CONTRACT_AREA" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """,
        "Relinquishment of Entire Contract area or part area (Upload Map of area being relinquished with co-ordinates/shape file to be uploaded)",
        36
    ),
    (
        """
        SELECT faao."REFID",cf.FILE_NAME,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cf.FILE_ID
        FROM dgh_staging.form_notice_next_phase_relin faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."SIG_DIGITAL_SIG" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = '1'
        """,
        "Signature / Digital Signature",
        608
    ),
    (
        """
        SELECT FAAO."REFID" ,CF.FILE_NAME ,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cfr.FILE_ID
        FROM dgh_staging.form_notice_next_phase_relin faao 
        JOIN dgh_staging.CMS_FILE_REF cfr 
        ON CFR.REF_ID = FAAO."FILEREF"
        JOIN dgh_staging.CMS_FILES cf 
        ON CFR.FILE_ID = CF.FILE_ID
        WHERE CFR.IS_ACTIVE = '1'
        """,
        "Additional File, If Any",
        39
    )
]


# === Execute All Queries ===
for query, label, label_id in queries:
    process_documents(query, label, label_id)

# === Close DB Connections ===
postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All documents uploaded successfully. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__