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

    # Execute the query directly from PostgreSQL
    pg_cursor.execute("""
		SELECT 
            faao."REFID",
			cf.file_label,
			cf.logical_doc_id,
			cf.label_id ,
			fcdfc.caliberation_flow_meters_witness_custody_details_id 
        FROM dgh_staging.form_calibratio_flow faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao."MIN_OF_MEETING" 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN upstream_data_management.t_caliberation_flow_meters_witness_custody_details fcdfc
            ON faao."REFID" = fcdfc.caliberation_flow_meters_witness_custody_details_application_nu
        WHERE faao."STATUS" = '1'
        
        union all
        
        SELECT 
            faao."REFID",
            cf.file_label,
			cf.logical_doc_id,
			cf.label_id ,
			fcdfc.caliberation_flow_meters_witness_custody_details_id 
        FROM dgh_staging.form_calibratio_flow faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao."SIG_WIT_CETI" 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN upstream_data_management.t_caliberation_flow_meters_witness_custody_details fcdfc
            ON faao."REFID" = fcdfc.caliberation_flow_meters_witness_custody_details_application_nu
        WHERE faao."STATUS" = '1'
        
        union all
        
        SELECT 
            faao."REFID",
            cf.file_label,
			cf.logical_doc_id,
			cf.label_id ,
			fcdfc.caliberation_flow_meters_witness_custody_details_id 
        FROM dgh_staging.form_calibratio_flow faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao."FILEREF"  
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN upstream_data_management.t_caliberation_flow_meters_witness_custody_details fcdfc
            ON faao."REFID" = fcdfc.caliberation_flow_meters_witness_custody_details_application_nu
        WHERE faao."STATUS" = '1'
        
        union all
        
        SELECT 
            faao."REFID",
            cf.file_label,
			cf.logical_doc_id,
			cf.label_id ,
			fcdfc.caliberation_flow_meters_witness_custody_details_id 
        FROM dgh_staging.form_calibratio_flow faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = faao."CALI_VER_AGR_NOT_FILE" 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN upstream_data_management.t_caliberation_flow_meters_witness_custody_details fcdfc
            ON faao."REFID" = fcdfc.caliberation_flow_meters_witness_custody_details_application_nu
        WHERE faao."STATUS" = '1'
    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} documents to insert")

    for refid, file_name, logical_doc_id, label_id, caliberation_flow_meters_witness_custody_details_id in rows:
        pg_cursor.execute("""
            INSERT INTO upstream_data_management.t_caliberation_flow_meters_witness_custody_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                caliberation_flow_meters_witness_custody_details_id
            ) VALUES (%s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_name, caliberation_flow_meters_witness_custody_details_id))

        print(f"📥 Inserted: REFID={refid}, LogicalDocID={logical_doc_id}, LabelID={label_id}, FileName={file_name}")

    pg_conn.commit()
    print("✅ All documents inserted successfully")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    print("🔚 PostgreSQL connection closed")
