import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect output to log file ===
sys.stdout = open("pg_calibration_flow_output.txt", "w", encoding="utf-8")
sys.stderr = sys.stdout

# === PostgreSQL Connection ===
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

# === Utility Function ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

# === Document Processing ===
def process_label(label_value, label_text, label_id):
    print(f"\n🔍 Starting processing for: {label_text}")
    
    query = f"""
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fcdfc.block_category,
            fcdfc.block_name,
            fcdfc.creation_date,
            cf.FILE_ID
        FROM dgh_staging.form_calibration_flow_sec faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao."LABEL_VALUE"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN upstream_data_management.t_caliberation_flow_meters_witness_custody_details fcdfc
            ON faao."REFID" = fcdfc.caliberation_flow_meters_witness_custody_details_application_nu
        WHERE faao."LABEL_TEXT" = '{label_value}' AND faao."STATUS" = '1'
    """
    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\n📄 File: {file_name}")
        print(f"RefID: {refid} | Block: {block} | Regime: {regime}")

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue

        try:
            with open(file_path, 'rb') as f:
                files = {'files': f}
                data = {
                    'regime': regime,
                    'block': block,
                    'module': 'Operator Contracts and Agreements',
                    'process': 'Calibration and Flow Meter Verification',
                    'financialYear': get_financial_year(created_on),
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
                else:
                    print(f"⚠️ docId not found in API response.")

        except Exception as err:
            print(f"❌ Upload failed for {file_name}: {err}")

# === Label Mapping ===
labels = [
    ("Brief_note_on_verification", 
     "Brief note on verification procedures and process description with layouts and PFD and As built latest P&ID S", 
     333),
     
    ("Identification_no_Tag_No", 
     "Identification no/Tag No and location details of the measuring devices must be provided. Provide last calibration details with seal nos.", 
     334),

    ("Calibration_data_certificates_of_PT", 
     "Identification no/Tag No and location details of the measuring devices. Calibration data & certificates of PT (Pressure Transmitters).", 
     363),

    ("Reports_of_Measurement", 
     "Reports of Measurement of Petroleum exercise must be certified / validated by third party agency and DGH representative (If present) and counter signed by operator.", 
     356)
    #  ,
    # ('','',),
    # ('','',),
    # ('','',),
    # ('','',),
    # ('','',),
]

# === Process All Labels ===
for label_value, label_text, label_id in labels:
    process_label(label_value, label_text, label_id)

postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All labels processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__
