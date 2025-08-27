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
        select site_restoration_abandonment_application_number, cf.file_label ,cf.logical_doc_id ,cf.label_id ,a.site_restoration_abandonment_details_id 
        from site_restoration.t_site_restoration_abandonment_details a
        join dgh_staging.form_site_restoration fsr on fsr."REF_ID" = a.site_restoration_abandonment_application_number 
        join dgh_staging.cms_file_ref cfr on cfr.ref_id in
        (fsr."UPLOAD_OCR" , fsr."SIG_DIGITAL_SIG", fsr."OCR_UNAVAIABLE_FILE")
        join dgh_staging.cms_files cf on cf.file_id  = cfr.file_id 
        where a.is_migrated = 1 and fsr."STATUS"  = 1 and fsr."IS_ACTIVE" = 1
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
