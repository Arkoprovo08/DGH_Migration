import os
import sys
import oracledb
import psycopg2
import requests
from datetime import datetime

# === Redirect stdout to log file ===
log_file = "migration_profit_petroleum_output.txt"
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

# === Upload Summary List ===
upload_summary = []

# === Utility Functions ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_document_status(document_name, status, doc_type_name, refid):
    try:
        insert_query = """
            INSERT INTO global_master.t_document_migration_status_details (
                document_name, document_migration_status, doc_type_name, refid
            ) VALUES (%s, %s, %s, %s)
        """
        postgres_cursor.execute(insert_query, (document_name, status, doc_type_name, refid))
        POSTGRES_CONN.commit()
    except Exception as log_err:
        print(f"⚠️ Failed to log status for {document_name}: {log_err}")

    # Add to summary
    upload_summary.append((document_name, status, doc_type_name, refid))

def process_documents(cursor, query, doc_type_name, label_id):
    cursor.execute(query)
    rows = cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\nProcessing {doc_type_name}:")
        print(regime, block, refid, sep='\n')

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            log_document_status(file_name, "File Not Found", doc_type_name, refid)
            continue

        files = {'files': open(file_path, 'rb')}
        data = {
            'regime': regime,
            'block': block,
            'module': 'Financial Management',
            'process': 'Cost and Profit Petroleum Calculations',
            'financialYear': get_financial_year(created_on),
            'referenceNumber': refid,
            'label': doc_type_name
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

                log_document_status(file_name, "Uploaded", doc_type_name, refid)
            else:
                print(f"⚠️ No docId found for {file_name} in responseObject")
                log_document_status(file_name, "Upload Failed - No docId", doc_type_name, refid)

        except Exception as upload_err:
            print(f"❌ Upload failed for {file_name}: {upload_err}")
            log_document_status(file_name, f"Upload Failed - {upload_err}", doc_type_name, refid)

# === Main Execution ===
try:
    oracle_conn = oracledb.connect(
        user=ORCL_USER,
        password=ORCL_PASSWORD,
        dsn=ORCL_DSN,
        mode=oracledb.SYSDBA
    )
    oracle_cursor = oracle_conn.cursor()
    print("✅ Connected to Oracle database.")

    # === Quarterly Statement ===
    query_scope = """
        SELECT faao.REFID,cf.FILE_NAME,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cf.FILE_ID
        FROM FRAMEWORK01.FORM_PROFIT_PETROLEUM faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.QUARTERLY = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(oracle_cursor, query_scope, "Quarterly Statement submitted as per DGH/PSC Format", 94)

    # === Additional Documents ===
    query_additional = """
        SELECT FAAO.REFID ,CF.FILE_NAME ,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cfr.FILE_ID
        FROM FRAMEWORK01.FORM_PROFIT_PETROLEUM faao 
        JOIN FRAMEWORK01.CMS_FILE_REF cfr 
        ON CFR.REF_ID = FAAO.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf 
        ON CFR.FILE_ID = CF.FILE_ID
        WHERE CFR.IS_ACTIVE = 1
    """
    process_documents(oracle_cursor, query_additional, "Additional File, If Any", 95)

    print("\n✅ All files processed.")

    # === Print Summary ===
    print("\n====== Upload Summary ======")
    print(f"{'Document Name':<40} {'Status':<30} {'Doc Type':<40} RefID")
    print("-" * 130)
    for doc_name, status, doc_type, refid in upload_summary:
        print(f"{doc_name:<40} {status:<30} {doc_type:<40} {refid}")

    # === Cleanup ===
    oracle_cursor.close()
    oracle_conn.close()
    print("\n✅ Oracle connection closed.")

    postgres_cursor.close()
    POSTGRES_CONN.close()
    print("✅ PostgreSQL connection closed.")

except Exception as err:
    print(f"❌ Error occurred: {err}")

# === Restore terminal output ===
sys.stdout.close()
sys.stdout = sys.__stdout__
