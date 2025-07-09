import os
import psycopg2
import requests
from datetime import datetime

# === Configuration ===
DB_CONFIG = {
    "host": "3.110.185.154",
    "port": 5432,
    "database": "ims",
    "user": "postgres",
    "password": "P0$tgres@dgh"
}

FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"


def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"


def upload_documents():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        fetch_query = """
            SELECT fwmn.ref_id, fwmn.dgh_letter, taad.block_category, taad.block_name, taad.creation_date
            FROM dgh_staging.frm_workitem_master_new fwmn
            JOIN inventory_tracking.t_inventory_report taad
            ON fwmn.ref_id = taad.inventory_report_application_number
            WHERE taad.is_migrated = 1
        """
        cursor.execute(fetch_query)
        records = cursor.fetchall()

        print(f"Found {len(records)} documents to upload...")

        for ref_id, file_name, block_category, block_name, created_on in records:
            file_path = os.path.join(FILES_DIR, file_name)
            print(f"\n📂 Processing file: {file_name} for REF_ID: {ref_id}")

            if not os.path.isfile(file_path):
                print(f"⚠️ File not found: {file_path}")
                continue

            files = {'files': open(file_path, 'rb')}
            data = {
                'regime': block_category,
                'block': block_name,
                'module': 'Inventory Tracking',
                'process': 'Inventory Reports',
                'financialYear': get_financial_year(created_on),
                'referenceNumber': ref_id,
                'label': 'Self Certificate'
            }

            try:
                response = requests.post(API_URL, files=files, data=data)
                print(f"📤 Uploaded file: {file_name}")
                response.raise_for_status()

                response_json = response.json()
                doc_id = None
                for item in response_json.get("responseObject", []):
                    if item.get("fileName") == file_name:
                        doc_id = item.get("docId")
                        break

                if doc_id:
                    print(f"✅ Success - Logical Doc ID: {doc_id}")

                    # Update Logical Doc ID in frm_workitem_master_new
                    update_fwmn = """
                        UPDATE dgh_staging.frm_workitem_master_new
                        SET LOGICAL_DOC_ID = %s
                        WHERE dgh_letter = %s
                    """
                    cursor.execute(update_fwmn, (doc_id, file_name))

                    # Update self certificate info in appointment_auditor_details
                    update_audit = """
                        UPDATE inventory_tracking.t_inventory_report
                        SET self_certificate_id = %s,
                            self_certificate_file_name = %s,
                            self_certificate_generation_date = NOW()
                        WHERE inventory_report_application_number = %s
                    """
                    cursor.execute(update_audit, (doc_id, file_name, ref_id))
                else:
                    print(f"⚠️ docId not found in response for: {file_name}")

            except Exception as err:
                print(f"❌ Error uploading {file_name}: {err}")

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ All files processed and committed to DB.")

    except Exception as db_err:
        print(f"❌ Database connection failed: {db_err}")


if __name__ == "__main__":
    upload_documents()
