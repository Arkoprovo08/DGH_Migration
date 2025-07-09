import os
import sys
import psycopg2
import requests
from datetime import datetime

log_file = "workflow_migration_output.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

POSTGRES_CONN = psycopg2.connect(
    host="3.110.185.154",
    port=5432,
    database="ims",
    user="postgres",
    password="P0$tgres@dgh"
)
postgres_cursor = POSTGRES_CONN.cursor()

API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
# FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"
FILES_DIR = r"C:\Users\vmrerauser08\Desktop\DGH_MIGRATION\documents"

def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def process_documents(cursor, query, label):
    cursor.execute(query)
    rows = cursor.fetchall()
    for refid, file_name, regime, block, created_on, file_id, process, module in rows:
        file_path = os.path.join(FILES_DIR, file_name)
        print(f"\n📄 Processing File: {file_name}")
        print(f"🔹 Regime: {regime}\n🔹 Block: {block}\n🔹 RefID: {refid}\n🔹 Module: {module}\n🔹 Process: {process}\n🔹 FY: {get_financial_year(created_on)} ")
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue
        with open(file_path, 'rb') as f:
            files = {'files': f}
            data = {
                'fileName': file_name,
                'referenceNumber': refid,
                'block': block,
                'regime': regime,
                'process': process,
                'module': module,
                'financialYear': get_financial_year(created_on),
                'label': label
            }
            try:
                response = requests.post(API_URL, files=files, data=data)
                print(files,data)
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
                        SET LOGICAL_DOC_ID = %s
                        WHERE FILE_ID = %s
                    """
                    cursor.execute(pg_update, (logical_doc_id, file_id))
                    POSTGRES_CONN.commit()
                else:
                    print(f"⚠️ No docId found for {file_name} in responseObject")
            except Exception as upload_err:
                print(f"❌ Upload failed for {file_name}: {upload_err}")

try:
    print("✅ Connected to PostgreSQL database.")
    query_workflow = """
        SELECT 
            iwmt.application_reference_number,
            cf.file_name,
            iwmt.regime,
            iwmt.block_name,
            cf.created_on,
            cf.file_id,
            mpm.process_longname,
            mpm.module_name
        FROM dgh_staging.inprogress_task_migration_temp faao
        JOIN dgh_staging.inprogress_workflow_migration_temp iwmt 
            ON faao.process_instance_id_task = CAST(iwmt.process_instance_id AS TEXT)
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf 
            ON faao.file_ref = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr 
            ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf 
            ON cf.FILE_ID = cfr.FILE_ID
        JOIN global_master.m_process_master mpm 
            ON iwmt.process_id = mpm.process_id
        WHERE cmf.ACTIVE = '1' 
            AND faao.process_id IS NOT NULL 
            AND faao.is_active = '1'
            and iwmt.is_active = '1'

    """
    process_documents(postgres_cursor, query_workflow, "Workflow Documents")
    print("✅ All files processed.")
except Exception as err:
    print(f"❌ Error occurred: {err}")
finally:
    postgres_cursor.close()
    POSTGRES_CONN.close()
    print("✅ PostgreSQL connection closed.")
    sys.stdout.close()
    sys.stdout = sys.__stdout__
