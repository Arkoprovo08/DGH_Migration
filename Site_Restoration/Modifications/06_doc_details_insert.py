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
			fcdfc.site_restoration_abandonment_application_number,
			cf.file_label,
			cf.logical_doc_id,
			cf.label_id,
			fcdfc.site_restoration_abandonment_details_id 
        FROM dgh_staging.form_site_restoration_data faao
        JOIN dgh_staging.CMS_FILE_REF cfr ON cfr.REF_ID = FAAO."DATA_VALUE" 
        JOIN dgh_staging.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        JOIN site_restoration.t_site_restoration_abandonment_details fcdfc ON faao."REFID" = fcdfc.site_restoration_abandonment_application_number
        WHERE faao."DATA_ID" in ('Btn_OISD','Btn_Third_Party','Btn_OC_approved') AND faao."STATUS" = 1
    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} documents to insert")

    for refid, file_name, logical_doc_id, label_id, site_restoration_abandonment_details_id in rows:
        pg_cursor.execute("""
            INSERT INTO site_restoration.t_site_restoration_abandonment_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                site_restoration_abandonment_details_id
            ) VALUES (%s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_name, site_restoration_abandonment_details_id))

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