import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect output to log file ===
sys.stdout = open("mop_log.txt", "w", encoding="utf-8")
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
FILES_DIR = r"C:\Users\dghvmuser05\Desktop\DGH FILES\LogicalDoc\PDF"

# === Utility Function ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def log_to_db(file_name, status, label_text, refid):
    try:
        postgres_cursor.execute('''
            INSERT INTO global_master.t_document_migration_status_details
            (document_name, document_migration_status, doc_type_name, refid)
            VALUES (%s, %s, %s, %s)
        ''', (file_name, status, label_text, refid))
        POSTGRES_CONN.commit()
    except Exception as log_error:
        print(f"❌ Logging failed for {file_name}: {log_error}")

# === Document Processing ===
def process_label(data_id, label_text, label_id):
    print(f"\n🔍 Processing: {label_text}")

    query = f'''
        select 
            fmp.refid,
            fmp.label_text,
            cf.file_name, 
            tmoph.block_category,
            tmoph.block_name,
            fmp.created_on,
            cf.file_id
        from dgh_staging.form_measurement_petroleum fmp  
        join dgh_staging.cms_file_ref cfr on fmp.label_value = cfr.ref_id 
        join dgh_staging.cms_files cf on cf.file_id = cfr.file_id
        join upstream_data_management.t_measurement_of_petroleum_header tmoph 
        on tmoph.measurement_of_petroleum_application_number = fmp.refid 
        where fmp.LABEL_TYPE = 'upload' 
        and fmp.label_text = %s
        and fmp.status = '1' and tmoph.is_migrated = 1
    '''

    postgres_cursor.execute(query, (data_id,))
    rows = postgres_cursor.fetchall()

    for refid, _, file_name, regime, block, created_by, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)
        print(f"\n📄 {file_name} | RefID: {refid} | Block: {block}")

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            log_to_db(file_name, "File not found", label_text, refid)
            continue

        try:
            with open(file_path, 'rb') as f:
                files = {'files': f}
                data = {
                    'regime': regime,
                    'block': block,
                    'module': 'Upstream Data Management',
                    'process': 'Measurement of Petroleum',
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
                    postgres_cursor.execute('''
                        UPDATE dgh_staging.CMS_FILES
                        SET LOGICAL_DOC_ID = %s, LABEL_ID = %s
                        WHERE FILE_ID = %s
                    ''', (logical_doc_id, label_id, file_id))
                    POSTGRES_CONN.commit()

                    log_to_db(file_name, "Uploaded", label_text, refid)
                else:
                    print("⚠️ docId not found in response.")
                    log_to_db(file_name, "docId not found in response", label_text, refid)

        except Exception as e:
            print(f"❌ Error uploading {file_name}: {e}")
            log_to_db(file_name, f"Upload error: {str(e)}", label_text, refid)

# === Labels List SIT ===

# data_labels = [
#     ("Additional Files if any", "Additional File, If Any", 30),
#     ("Allocation method", "Allocation method", 23),
#     ("Attach PFD and mention number points where Petroleum measurement to be done with in contract boundary", "Attach PFD and mention number points where Petroleum measurement to be done with in contract boundary", 21),
#     ("Complete flow diagram of production facility with measurement points and storage points starting from well heads to delivery points ", "Complete flow diagram of production facility with measurement points and storage points starting from well heads to delivery points", 27),
#     ("Details of API/AGA/ASTM/ISO Standards followed for Measurement System Design.", "Details of API/AGA/ASTM/ISO Standards followed for Measurement System Design", 26),
#     ("Details of buyer (noofbuyers) to be included,(including Type of Meter, Make, Size, Range Percentage Error, frequency of calibration)", "Details of buyer I to be included,(including Type of Meter, Make, Size, Range Percentage Error, frequency of calibration)", 18),
#     ("Details of meters installed at site for measurement of fluids with model, make, specifications and OEM details.", "Details of meters installed at site for measurement of fluids with model, make, specifications and OEM details", 28),
#     ("Dip tape detail", "Dip Tape Detail", 17),
#     ("Frequency of Tank gauging", "Frequency of Tank gauging", 16),
#     ("Method to be employed for measurement of volume of Petroleum production", "Method to be employed for measurement of volume of Petroleum production", 19),
#     ("Operator to provide recommended frequency of inspection as per general standard of each meter and custody transfer meter", "Operator to provide recommended frequency of inspection as per general standard of each meter and custody transfer meter", 24),
#     ("Provide procedure of calibration/testing for each measurement device used in MOP exercise.", "Provide procedure of calibration/testing for each measurement device used in MOP exercise", 25),
#     ("Schematics related to metering skids, storage tanks or Measurement of Petroleum etc.", "Schematics related to metering skids, storage tanks or Measurement of Petroleum etc", 29),
#     ("Submit COSA and GSA signed with each party", "Submit COSA and GSA signed with each party", 22),
#     ("The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract.", "The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract", 20),
#     ("The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the terms of this contract.", "The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract", 20),
#     ("Upload OCR ", "Upload OCR", 13),
#     ("As built drawing related to metering skids, storage tanks or Measurement of Petroleum etc.", "As built drawing related to metering skids, storage tanks or Measurement of Petroleum etc.", 505),
#     ("Name of third party and Year of experience", "Name of third party and Year of experience", 506),
#     ("UPLOAD OVERVIEW OF BLOCK", "UPLOAD OVERVIEW OF BLOCK", 507)
# ]

# === Labels List (DEV) ===

data_labels = [
    ("Additional Files if any", "Additional File, If Any", 30),
    ("Allocation method", "Allocation method", 23),
    ("Attach PFD and mention number points where Petroleum measurement to be done with in contract boundary", "Attach PFD and mention number points where Petroleum measurement to be done with in contract boundary", 21),
    ("Complete flow diagram of production facility with measurement points and storage points starting from well heads to delivery points ", "Complete flow diagram of production facility with measurement points and storage points starting from well heads to delivery points", 27),
    ("Details of API/AGA/ASTM/ISO Standards followed for Measurement System Design.", "Details of API/AGA/ASTM/ISO Standards followed for Measurement System Design", 26),
    ("Details of buyer (noofbuyers) to be included,(including Type of Meter, Make, Size, Range Percentage Error, frequency of calibration)", "Details of buyer I to be included,(including Type of Meter, Make, Size, Range Percentage Error, frequency of calibration)", 18),
    ("Details of meters installed at site for measurement of fluids with model, make, specifications and OEM details.", "Details of meters installed at site for measurement of fluids with model, make, specifications and OEM details", 28),
    ("Dip tape detail", "Dip Tape Detail", 17),
    ("Frequency of Tank gauging", "Frequency of Tank gauging", 16),
    ("Method to be employed for measurement of volume of Petroleum production", "Method to be employed for measurement of volume of Petroleum production", 19),
    ("Operator to provide recommended frequency of inspection as per general standard of each meter and custody transfer meter", "Operator to provide recommended frequency of inspection as per general standard of each meter and custody transfer meter", 24),
    ("Provide procedure of calibration/testing for each measurement device used in MOP exercise.", "Provide procedure of calibration/testing for each measurement device used in MOP exercise", 25),
    ("Schematics related to metering skids, storage tanks or Measurement of Petroleum etc.", "Schematics related to metering skids, storage tanks or Measurement of Petroleum etc", 29),
    ("Submit COSA and GSA signed with each party", "Submit COSA and GSA signed with each party", 22),
    ("The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract.", "The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract", 20),
    ("The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the terms of this contract.", "The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract", 20),
    ("Upload OCR ", "Upload OCR", 13),
    ("As built drawing related to metering skids, storage tanks or Measurement of Petroleum etc.", "As built drawing related to metering skids, storage tanks or Measurement of Petroleum etc.", 609),
    ("Name of third party and Year of experience", "Name of third party and Year of experience", 610),
    ("UPLOAD OVERVIEW OF BLOCK", "UPLOAD OVERVIEW OF BLOCK", 611)
]


for data_id, label_text, label_id in data_labels:
    process_label(data_id, label_text, label_id)

postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All labels processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__