import os
import sys
import psycopg2
import requests
from datetime import datetime

log_file = "migration_appraisal_output.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

POSTGRES_CONN = psycopg2.connect(
    host="13.127.174.112",
    port=5432,
    database="ims",
    user="imsadmin",
    password="Dghims!2025"
)
postgres_cursor = POSTGRES_CONN.cursor()

API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"

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
            'module': 'Operator Contracts and Agreements',
            'process': 'Appraisal Programme',
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
    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."DISCOVERY_DETAILS" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "PML Area Map", 122)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."APPRISAL_PLAN" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Submit the details of the discovery for which the appraisal plan is submitted. Describe the discovery in terms of date of notification, date of submission of Potential Commercial Interest, encountered reservoirs with corresponding zone intervals (metre), formation name, hydrocarbon flow rate, bean size and flowing pressure [200 words]:", 123)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."MC_RESOLUTION" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Furnish the MC resolution that reviewed the location which has been notified as the said discovery [Step 1.1]:", 124)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."APPRISAL_AREA_MAP" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Read with the submission through Step 1.3, submit base map(s), showing areas/ wells where activities are proposed to be carried out within the appraisal (discovery) area:", 126)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."WELLS_LIST" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "List out the wells in the contract area (block) with well name, surface latitude, surface longitude, subsurface latitude, subsurface longitude, drilled depth(metre), status (oil, gas, oil+gas or abandoned) and specify the discovery well:", 127)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."DRILLED_WELLS" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Scaled base map showing all drilled wells and operational area:", 128)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SCALE_STRUCTURE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Scaled top structure map of the reservoir(s) to be appraised showing all drilled wells with status and appraisal(discovery) area demarcated:", 129)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."LIST_OUT_SEISMIC" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "List out seismic 2D and/or 3D data with quantum, nature, vintage and coverage polygon coordinates:", 130)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SCALE_SEISMIC" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Scaled seismic coverage map (2D and/or 3D) vis-à-vis operational and proposed appraisal area:", 131)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."NATURE_SEISMIC" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "List out nature of seismic/ well datasets used in the Appraisal Plan like amplitude, attributes, inversion, velocity, time-depth relationship:", 132)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SUITABLY_SEISMIC" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Suitably scaled seismic sections along inline, crossline and along discovery and offset wells:", 133)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SUITABLY_WELL" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Suitably scaled well log correlation along discovery and offset wells:", 134)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."LIST_OUT_WELLS" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "List out the wells used in the Appraisal Plan and also those proposed:", 135)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."CONSIDERED_APPRAISAL" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "List out total number of reservoirs envisaged and specify the ones considered for the appraisal:", 136)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SCALED_REPRESENTATIVE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Scaled representative well log section showing the reservoir zones, suitably annotated with legend - one well per sheet:", 137)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SUBMIT_PETROPHYSICAL" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Submit petrophysical summation table showing zone details and petrophysical parameters for discovery well: ", 138)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SCALED_TOP_STRUCTURE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Scaled top structure map, superimposed with gross thickness/ seismic attribute map and drilled/ proposed wells, all suitably annotated with legend:", 139)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SUPPORTING_ESTIMATE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "If other option is chosen in1.6.1 then  state what estimation approach has been used [200 words]? Provide all data supporting such estimate", 140)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."LIST_OUT_WELL_WISE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "List Out Well Deviation Survey Data For Inclined Wells", 142)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SEISMIC_DATA" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "2D seismic data", 149)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SEISMIC_CUBE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "3D seismic cube in PSTM/PSDM (Time and Depth)", 150)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SEISMIC_HORIZONS" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "All seismic horizons interpreted", 151)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SEISMIC_ATTRIBUTE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "All seismic attribute maps", 153)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."VELOCITY_MODEL" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Velocity model", 154)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."SEISMIC_INVERSION_CUBE" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Seismic inversion cube, if used", 155)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."CROSS_PLOTS" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Seismic inversion cross-plots, if used", 156)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."AVO_AVA" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "AVO/AVA analysis, if used", 157)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."MAP_BASED_VOLUMETRIC" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "For map-based volumetric approach, place an index note on the approach [200 words]. Submit top structure and gross thickness maps.", 158)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."WELL_BASED_VOLUMETRIC" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "For well-based volumetric approach, place an index note on the approach [200 words]. Submit well sections of all wells considered showing zone intervals, thickness (gross and net) along with supporting maps showing the area of assessment [200 words]", 159)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."BRIEF_ON_GEOLOGY" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Submit a brief on geology of the area describing tectonics, depositional environment, stratigraphy, reservoir development and petroleum system with particular reference to Indian and/or global analogy [500 words]", 160)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."REPORT_CORE_STUDIES" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "Core studies", 164)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            fap."BLOCKCATEGORY",
            fap."BLOCKNAME",
            fap."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme_bt faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."FAULTS_DISCONTINUITIES" = cfr.ref_id 
        JOIN dgh_staging.form_appraisal_programme fap ON fap."REFID" = faao."REFID"
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE fap."IS_ACTIVE" = '1' AND faao."IS_ACTIVE" = '1' AND cfr.is_active = '1'
    """
    process_documents(postgres_cursor, query_scope, "All faults/ discontinuities interpreted", 152)

    query_scope = """
        SELECT 
            faao."REFID",
            cf.FILE_NAME,
            faao."BLOCKCATEGORY",
            faao."BLOCKNAME",
            faao."CREATED_ON",
            cf.FILE_ID
        FROM dgh_staging.form_appraisal_programme faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON faao."UPLOAD_OCR" = cfr.ref_id 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = faao."REFID"
        WHERE faao."IS_ACTIVE" = '1' and cfr.is_active = '1'  
    """
    process_documents(postgres_cursor, query_scope, "Upload OCR", 121)

    # Upload Additional Documents 
    query_additional = """
        SELECT FAAO."REFID" ,CF.file_name ,CF.file_label ,faao."BLOCKCATEGORY",faao."BLOCKNAME",faao."CREATED_ON",cfr.FILE_ID
        FROM dgh_staging.form_appraisal_programme faao 
        JOIN dgh_staging.CMS_FILE_REF cfr 
        ON CFR.REF_ID = FAAO."FILEREF"
        JOIN dgh_staging.CMS_FILES cf 
        ON CFR.FILE_ID = CF.FILE_ID
        JOIN dgh_staging.frm_workitem_master_new fwmn
        ON fwmn.ref_id = faao."REFID"
        WHERE CFR.IS_ACTIVE = 1 and cf.is_active = 1 and faao."IS_ACTIVE" = '1'
        
    """
    process_documents(postgres_cursor, query_additional, "Upload additional documents", 174)

    print("✅ All files processed.")

    # === Cleanup ===
    postgres_cursor.close()
    POSTGRES_CONN.close()
    print("✅ PostgreSQL connection closed.")

except Exception as err:
    print(f"❌ Error occurred: {err}")

sys.stdout.close()
sys.stdout = sys.__stdout__