import psycopg2

# PostgreSQL DB Connection
pg_conn = psycopg2.connect(
    host="13.127.174.112",
    port=5432,
    database="ims",
    user="imsadmin",
    password="Dghims!2025"
)

try:
    pg_cursor = pg_conn.cursor()
    print("✅ Connected to PostgreSQL")

    # Fetch document metadata related to Format C
    pg_cursor.execute("""
select distinct faao."REFID",cf.file_label,cf.logical_doc_id,cf.label_id,
       tbgd.bank_gurantee_details_id, null::int as bank_gurantee_header_id
FROM dgh_staging.FORM_SUB_BG_LEGAL_RENEWAL faao
JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."UPLOAD_LEGAL_OPINION" = cmf.FILEREF
JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
join financial_mgmt.t_bank_gurantee_header taad on taad.bank_gurantee_application_number = faao."REFID"
join financial_mgmt.t_bank_gurantee_details tbgd on tbgd.bank_gurantee_header_id = taad.bank_gurantee_header_id
WHERE cmf.ACTIVE = '1' and faao."STATUS" = 1 and cmf.active = '1' and tbgd.migration_seq = faao."SEQ"

union all

select distinct faao."REFID",cf.file_label,cf.logical_doc_id,cf.label_id,
       tbgd.bank_gurantee_details_id, null::int as bank_gurantee_header_id
FROM dgh_staging.FORM_SUB_BG_LEGAL_RENEWAL faao
JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."UPLOAD_BG" = cmf.FILEREF
JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
join financial_mgmt.t_bank_gurantee_header taad on taad.bank_gurantee_application_number = faao."REFID"
join financial_mgmt.t_bank_gurantee_details tbgd on tbgd.bank_gurantee_header_id = taad.bank_gurantee_header_id
WHERE cmf.ACTIVE = '1' and faao."STATUS" = 1 and cmf.active = '1' and tbgd.migration_seq = faao."SEQ"

union all

select distinct faao."REFID",cf.file_label,cf.logical_doc_id,cf.label_id,
       tbgd.bank_gurantee_details_id, null::int as bank_gurantee_header_id
FROM dgh_staging.FORM_SUB_BG_LEGAL_RENEWAL faao
JOIN dgh_staging.CMS_MASTER_FILEREF cmf ON faao."PREV_BG_LINKED_DET" = cmf.FILEREF
JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
join financial_mgmt.t_bank_gurantee_header taad on taad.bank_gurantee_application_number = faao."REFID"
join financial_mgmt.t_bank_gurantee_details tbgd on tbgd.bank_gurantee_header_id = taad.bank_gurantee_header_id
WHERE cmf.ACTIVE = '1' and faao."STATUS" = 1 and cmf.active = '1' and tbgd.migration_seq = faao."SEQ"

union all

SELECT FAAO."REFID" ,CF.file_label , cf.logical_doc_id, cf.label_id,
       null::int as bank_gurantee_details_id, taad.bank_gurantee_header_id
FROM dgh_staging.FORM_SUB_BG_LEGAL_RENEWAL faao 
JOIN dgh_staging.CMS_FILE_REF cfr ON CFR.REF_ID = FAAO."FILEREF"
JOIN dgh_staging.CMS_FILES cf ON CFR.FILE_ID = CF.FILE_ID
join financial_mgmt.t_bank_gurantee_header taad on taad.bank_gurantee_application_number = faao."REFID"
WHERE CFR.IS_ACTIVE = 1;

    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} Format C document records to insert")

    for app_no, file_label, logical_doc_id, label_id, bank_gurantee_details_id, bank_gurantee_header_id in rows:
        pg_cursor.execute("""
            INSERT INTO financial_mgmt.t_bank_gurantee_document_details 
            (
                document_ref_number,
                document_type_id,
                document_name,
                bank_gurantee_details_id,
                bank_gurantee_header_id
            ) VALUES (%s, %s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_label, bank_gurantee_details_id, bank_gurantee_header_id))

        print(f"📥 Inserted: AppNo={app_no}, LogicalDocID={logical_doc_id}, LabelID={label_id}, "
              f"FileLabel={file_label}, FormatC_ID={bank_gurantee_details_id}, FormatC_Well_ID={bank_gurantee_header_id}")
    
    pg_conn.commit()
    print("✅ All Format C document details inserted successfully")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    print("🔚 PostgreSQL connection closed")
