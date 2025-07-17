import os
import sys
import psycopg2
import requests
from datetime import datetime

# === Redirect output to log file ===
sys.stdout = open("assignment_interest_upload_log.txt", "w", encoding="utf-8")
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
FILES_DIR = r"C:\\Users\\Administrator.DGH\\Desktop\\dgh\\Files\\CMS\\Uploads"

# === Utility Function ===
def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

# === Document Processing ===
def process_label(data_id, label_text, label_id):
    print(f"\n🔍 Processing: {label_text}")

    query = f'''
        SELECT 
            cmf.refid,
            faao.data_id ,
            cf.FILE_NAME,
            fcdfc.BLOCKCATEGORY,
            fcdfc.BLOCKNAME,
            fcdfc.CREATED_BY,
            cf.FILE_ID
        FROM dgh_staging.form_assignment_interest_data faao
        JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao.DATA_VALUE = cmf.FILEREF
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.form_assignment_interest fcdfc ON fcdfc.ref_id  = faao.REFID
        WHERE cmf.ACTIVE = '1' AND faao.DATA_ID = '{data_id}' AND faao.STATUS = '1' and fcdfc.status = '1'
        ORDER BY faao.refid, faao.data_id;
    '''

    postgres_cursor.execute(query)
    rows = postgres_cursor.fetchall()

    for refid, _, file_name, regime, block, created_by, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)
        print(f"\n📄 {file_name} | RefID: {refid} | Block: {block}")

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
                    'process': 'Assignment/Transfer of Participating Interest',
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
                else:
                    print("⚠️ docId not found in response.")

        except Exception as e:
            print(f"❌ Error uploading {file_name}: {e}")

# === Labels List ===
data_labels = [
    ("btn_addition_file", "Upload additional documents", 489),
    ("btn_Amendments", "Draft Amendments to the PSC submitted", 475),
    ("btn_Annual_Audited", "Copy of signed Annual Audited Financial Statements of the Parent Company, if guarantor", 473),
    ("btn_article_28", "Undertakings by assignor (s) as per Article 28.1", 488),
    ("btn_authorising_transfer", "Assignor Board resolution authorising the transfer", 459),
    ("btn_Bank_Guarantee_by", "Bank Guarantee by the assignee company subsequent to the approval", 460),
    ("btn_Board_authorising", "Assignee Board resolution authorising the transfer", 458),
    ("btn_Board_resolution", "Copy of Board Resolution by the assignor company to assign PI in the block", 465),
    ("btn_Brief_corporate", "Brief on the corporate, technical capability & other information of the assignee", 461),
    ("btn_Copy_assignment", "Copy of assignment and assumption deed executed by the assignor (s) & assignee (s)", 462),
    ("btn_Copy_Board", "Copy of Board Resolution by the assignee company to assume PI from assignor", 463),
    ("btn_Copy_operating_Committee", "Copy of operating Committee Resolution (Management committee resolution...", 467),
    ("btn_Copy_signed", "Copy of signed Annual Audited Financial Statements / Printed Annual Reports for the preceding three years", 470),
    ("btn_Copy_signed_Annual", "Copy of signed Annual Audited Financial Statements / Printed Annual Reports for the preceding three years Assignee", 471),
    ("btn_Deed_partnership", "Deed of Assignment and Assumption submitted", 474),
    ("btn_draft_amendment", "Draft Amendment to PSC", 476),
    ("btn_Ex_Phase_I", "Exploration Phase-I / Initial Exploration Period", 478),
    ("btn_Ex_Phase_II", "Exploration Phase-II / Subsequent Exploration Period", 479),
    ("btn_Ex_Phase_III", "Exploration Phase-III", 477),
    ("btn_Family_tree", "Family tree of the parent company (in case of assignment to affiliates)", 481),
    ("btn_Financial_Performance", "Financial & Performance Guarantee by the Parent company else assignee", 482),
    ("btn_Memorandum", "A copy of Memorandum and Articles of Association / Certificate of incorporation of the assignee", 456),
    ("btn_No_objection_certificate", "No objection certificate from the consortium partners", 483),
    ("btn_Operating_committee_resolution", "Operating committee resolution for change of operator ship including licensee", 484),
    ("btn_PI_transfered", "PI transfered to ‘Affiliate’ or other related party are accordance of Article 28 of the PSC", 486),
    ("btn_power_attorney", "Copy of power of attorney / authority letter...on behalf of assignee (s)", 468),
    ("btn_Undertakings", "Undertakings by assignee (s) as per Article 28.1", 487)
]

for data_id, label_text, label_id in data_labels:
    process_label(data_id, label_text, label_id)

postgres_cursor.close()
POSTGRES_CONN.close()
print("\n✅ All labels processed. PostgreSQL connection closed.")
sys.stdout.close()
sys.stdout = sys.__stdout__