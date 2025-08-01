import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect output to log file ===
sys.stdout = open("pg_only_migration_output.txt", "w", encoding="utf-8")
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
FILES_DIR = r"\\192.168.0.126\it\CMS\Uploads1"

# === Utility Functions ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
    elif isinstance(created_on, datetime):
        pass
    else:
        raise ValueError("Invalid datetime input for financial year")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_to_db(file_name, status, label_text, refid):
    try:
        insert_query = """
            INSERT INTO global_master.t_document_migration_status_details 
            (document_name, document_migration_status, doc_type_name, refid)
            VALUES (%s, %s, %s, %s)
        """
        postgres_cursor.execute(insert_query, (file_name, status, label_text, refid))
        POSTGRES_CONN.commit()
    except Exception as e:
        print(f"❌ Failed to log status for {file_name}: {e}")

# === Document Processing ===
def process_label(label_value, label_text, label_id):
    print(f"\n🔍 Starting processing for: {label_text}")
    
    query = f"""
        SELECT 
            cmf.refid,
            cf.FILE_NAME,
            fcdfc.block_category,
            fcdfc.block_name,
            fcdfc.creation_date,
            cf.FILE_ID
        FROM dgh_staging.form_inventory_report faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."LABEL_VALUE" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN inventory_tracking.t_inventory_report fcdfc ON faao."REFID" = fcdfc.inventory_report_application_number
        WHERE cmf.ACTIVE = '1' AND faao."LABEL_TEXT" = '{label_value}' AND faao."STATUS" = '1'
    """
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\n📄 File: {file_name}")
        print(f"RefID: {refid} | Block: {block} | Regime: {regime}")

        if not os.path.exists(file_path):
            msg = f"❌ File not found: {file_path}"
            print(msg)
            log_to_db(file_name, "File Not Found", label_text, refid)
            continue

        try:
            with open(file_path, 'rb') as f:
                files = {'files': f}
                try:
                    fy = get_financial_year(created_on)
                except Exception as fy_err:
                    print(f"⚠️ Could not determine financial year: {fy_err}")
                    fy = "NA"

                data = {
                    'regime': regime,
                    'block': block,
                    'module': 'Operator Contracts and Agreements',
                    'process': 'Inventory Report',
                    'financialYear': fy,
                    'referenceNumber': refid,
                    'label': label_text
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
                    log_to_db(file_name, "Success", label_text, refid)
                else:
                    print("⚠️ docId not found in API response.")
                    log_to_db(file_name, "docId Not Found", label_text, refid)

        except Exception as err:
            print(f"❌ Upload failed for {file_name}: {err}")
            log_to_db(file_name, f"Upload Failed: {err}", label_text, refid)

# === Updated Labels SIT===
# labels = [
#     ("Upload inventory list for other assets", "Upload inventory list for other assets", 257),
#     ("Upload Report", "Upload Report", 258),
#     ("Additional File, If Any", "Additional File, If Any", 259),
#     ("Upload inventory list for fixed assets", "Upload inventory list for fixed assets", 260)
# ]


# === Labels (DEV) ===
labels = [
    ("Upload inventory list for other assets", "Upload inventory list for other assets", 570),
    ("Upload Report", "Upload Report", 568),
    ("Additional Files if any", "Additional File, If Any", 569),
    ("Upload inventory list for fixed assets", "Upload inventory list for fixed assets", 570)
]

# === Run All Labels ===
for label_value, label_text, label_id in labels:
    process_label(label_value, label_text, label_id)

postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All labels processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__
