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
            cmf.refid,
            cf.FILE_NAME,
            fcdfc.block_category,
            fcdfc.block_name,
            fcdfc.created_by,
            cf.FILE_ID,
            tfcwd.well_name_or_number
        FROM dgh_staging.form_commerical_dis_format_c2 faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.label_input = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN operator_contracts_agreements.t_format_c_commercial_discovery_header fcdfc ON faao.refid = fcdfc.format_c_application_no
        JOIN operator_contracts_agreements.t_format_c_well_details tfcwd ON tfcwd.format_c_id = fcdfc.format_c_id
        WHERE cmf.ACTIVE = '1' AND faao.label_value = '{label_value}' AND faao.status = '1'
    """

    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, file_name, regime, block, created_by, file_id, well_name in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\n📄 File: {file_name}")
        print(f"RefID: {refid} | Block: {block} | Well: {well_name}")

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
                    'process': 'Format-C: Commercial Discovery (Declaration of Commerciality)',
                    'financialYear': get_financial_year(created_by),
                    'referenceNumber': refid,
                    'label': label_text,
                    'well': well_name
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

labels = [
    ("Zone_and_perforated_interval_tested", "Zone and perforated interval tested", 46),
    ("Well_wise_and _zone_wise_petrophysical", "Well_wise and  zone_wise petrophysical summation table incorporating gross thickness,pay thickness, porosity and water saturation with cut-off", 47),
    ("Results_of_formation_evaluation_test", "Results of formation evaluation test (MDT/XPT/RFT) (If any); Zone thickness", 48),
    ("Results_of_Conventional_Testing", "Results of Conventional Testing including DST (if any)", 49),
    ("Production_rate", "Production rate (Oil, Gas Condensate & Water ) with various choke sizes", 50),
    ("Reservoir_Pressure_and_Temperature", "Reservoir Pressure and Temperature", 51),
    ("Well_productivity_index", "Well productivity index/AOFP", 52),
    ("Pressure_transien_and_surveillance", "Pressure transient and surveillance studies, interpretation, analysis & Reports", 53),
    ("Fluid_properties", "Fluid properties/PVT properties", 54),
    ("Results_of_routine", "Results of routine core analysis (porosity, permeability, grain density, capillary pressure, etc.)", 55),
    ("Fluid_contact", "Fluid contact(s), OWC, GOC", 56)
]

for label_value, label_text, label_id in labels:
    process_label(label_value, label_text, label_id)
postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All labels processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__
