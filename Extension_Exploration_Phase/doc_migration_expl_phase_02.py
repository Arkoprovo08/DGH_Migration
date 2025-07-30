import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect output to log file ===
sys.stdout = open("exploration_phase_migration_log.txt", "w", encoding="utf-8")
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
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_status(document_name, status, doc_type_name, refid):
    try:
        postgres_cursor.execute("""
            INSERT INTO global_master.t_document_migration_status_details
            (document_name, document_migration_status, doc_type_name, refid)
            VALUES (%s, %s, %s, %s)
        """, (document_name, status, doc_type_name, refid))
        POSTGRES_CONN.commit()
    except Exception as log_err:
        print(f"⚠️ Failed to log for {document_name}: {log_err}")

# === Document Processing ===
def process_label(data_id, label_text, label_id):
    print(f"\n🔍 Processing: {label_text}")

    query = f'''
        SELECT 
            cmf.refid,
            cf.FILE_NAME,
            fcdfc."BLOCKCATEGORY",
            fcdfc."BLOCKNAME",
            fcdfc."CREATED_BY",
            cf.FILE_ID
        FROM dgh_staging.form_extension_phase_data faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."DATA_VALUE" = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.form_extension_explo_phase fcdfc ON fcdfc."REF_ID" = faao."REFID"
        WHERE cmf.ACTIVE = '1' AND faao."DATA_ID" = '{data_id}' AND faao."STATUS" = '1'
    '''

    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_by, file_id in rows:
        print(f"\n📄 {file_name} | RefID: {refid} | Block: {block}")

        if not file_name:
            print("⚠️ File name is NULL.")
            log_status(None, 'No self certificate', label_text, refid)
            continue

        file_path = os.path.join(FILES_DIR, file_name)

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            log_status(file_name, 'File Not Found', label_text, refid)
            continue

        try:
            with open(file_path, 'rb') as f:
                files = {'files': f}
                data = {
                    'regime': regime,
                    'block': block,
                    'module': 'Exploration Phase Extension',
                    'process': 'Request for Extension of Exploration Phase',
                    'financialYear': get_financial_year(created_by),
                    'referenceNumber': refid,
                    'label': label_text
                }

                response = requests.post(API_URL, files=files, data=data)
                print(f"🌐 API Response: {response.text}")
                response.raise_for_status()

                response_json = response.json()
                response_objects = response_json.get("responseObject", [])
                logical_doc_id = next((obj.get("docId") for obj in response_objects if obj.get("fileName") == file_name), None)

                if logical_doc_id:
                    print(f"✅ Uploaded. docId: {logical_doc_id}")
                    update_query = '''
                        UPDATE dgh_staging.CMS_FILES
                        SET LOGICAL_DOC_ID = %s, LABEL_ID = %s
                        WHERE FILE_ID = %s
                    '''
                    postgres_cursor.execute(update_query, (logical_doc_id, label_id, file_id))
                    POSTGRES_CONN.commit()

                    log_status(file_name, 'Uploaded', label_text, refid)

                else:
                    print("⚠️ docId not found in response.")
                    log_status(file_name, 'docId Missing in Response', label_text, refid)

        except Exception as e:
            print(f"❌ Error uploading {file_name}: {e}")
            log_status(file_name, 'Upload Failed', label_text, refid)

# === Labels List ===
data_labels = [
    ("Btn_Payment_Document", "Payment Document", 78),
    ("Btn_Calculation_ld", "Calculation for LD", 79),
    ("Btn_Bank_Guarantee", "Please upload Bank Guarantee", 492),
    ("Btn_Purpose", "Purpose of Bank Guarantee", 493),
    ("Btn_compute", "Cost of remaining/additional work programme", 494),
    ("Btn_Calculation", "Calculation of Bank Guarantee", 495),
    ("Btn_Extension_Exploration", "Submit Representation", 72),
    ("Btn_Legal_Opinion", "UPLOAD LEGAL OPINION", 497)
]

# === Run All Labels ===
for data_id, label_text, label_id in data_labels:
    process_label(data_id, label_text, label_id)

# === Cleanup ===
postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All labels processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__
