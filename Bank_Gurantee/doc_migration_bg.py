import os
import sys
import oracledb
import psycopg2
import requests
from datetime import datetime

# === Redirect stdout to log file ===
log_file = "migration_output.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

# === Oracle DB Config ===
ORCL_USER = "sys"
ORCL_PASSWORD = "Dgh12345"
ORCL_HOST = "192.168.0.133"
ORCL_PORT = 1521
ORCL_SID = "ORCL"
ORCL_DSN = oracledb.makedsn(ORCL_HOST, ORCL_PORT, sid=ORCL_SID)

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
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def process_documents(cursor, query, label, label_id):
    cursor.execute(query)
    rows = cursor.fetchall()

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
            'process': 'Bank Guarantee and Legal Opinion, and renewal and revised Bank Guarantee',
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

                # ✅ Update PostgreSQL table
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
    # === Connect to Oracle DB ===
    oracle_conn = oracledb.connect(
        user=ORCL_USER,
        password=ORCL_PASSWORD,
        dsn=ORCL_DSN,
        mode=oracledb.SYSDBA
    )
    oracle_cursor = oracle_conn.cursor()
    print("✅ Connected to Oracle database.")

    # === Legal Opinion ===
    query_scope = """
        SELECT faao.REFID,cf.FILE_NAME,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cf.FILE_ID
        FROM FRAMEWORK01.FORM_SUB_BG_LEGAL_RENEWAL faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.UPLOAD_LEGAL_OPINION = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(oracle_cursor, query_scope, "Upload Legal Opinion Document", 10)

    # === Upload BG ===
    query_ocr = """
        SELECT faao.REFID,cf.FILE_NAME,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cf.FILE_ID
        FROM FRAMEWORK01.FORM_SUB_BG_LEGAL_RENEWAL faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.UPLOAD_BG = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(oracle_cursor, query_ocr, "Upload Bank Guarantee (The original copy to be submitted to DGH within 7 working days from date of uploading)", 11)

    # === Upload Previous BG ===
    query_mc = """
        SELECT faao.REFID,cf.FILE_NAME,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cf.FILE_ID
        FROM FRAMEWORK01.FORM_SUB_BG_LEGAL_RENEWAL faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.PREV_BG_LINKED_DET = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(oracle_cursor, query_mc, "Upload Previous BG Document", 9)

    # === Signature Digital Signature ===
    query_ocr_no = """
        SELECT faao.REFID,cf.FILE_NAME,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cf.FILE_ID
        FROM FRAMEWORK01.FORM_SUB_BG_LEGAL_RENEWAL faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.SIG_DIGITAL_SIG = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(oracle_cursor, query_ocr_no, "Signature/ Digital signature", 250)

    # === Additional Documents ===
    query_additional = """
        SELECT FAAO.REFID ,CF.FILE_NAME ,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cfr.FILE_ID
        FROM FRAMEWORK01.FORM_SUB_BG_LEGAL_RENEWAL faao 
        JOIN FRAMEWORK01.CMS_FILE_REF cfr 
        ON CFR.REF_ID = FAAO.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf 
        ON CFR.FILE_ID = CF.FILE_ID
        WHERE CFR.IS_ACTIVE = 1
    """
    process_documents(oracle_cursor, query_additional, "Additional File, If Any", 12)

    print("✅ All files processed.")

    # === Cleanup ===
    oracle_cursor.close()
    oracle_conn.close()
    print("✅ Oracle connection closed.")

    postgres_cursor.close()
    POSTGRES_CONN.close()
    print("✅ PostgreSQL connection closed.")

except Exception as err:
    print(f"❌ Error occurred: {err}")

# === Restore terminal output (optional) ===
sys.stdout.close()
sys.stdout = sys.__stdout__