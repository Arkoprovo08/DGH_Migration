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

    # Execute query for both 'UPLOAD_OCR' and 'WELL_TYPE_FILE' based documents
    pg_cursor.execute("""
        SELECT a."WEL_LOC_CHA_DEEP_ID",
               cf.file_label,
               cf.logical_doc_id,
               cf.label_id,
               w.well_location_well_details_id,
               w.well_location_header_id
        FROM dgh_staging.form_wel_loc_cha_de_data a
        JOIN operator_contracts_agreements.t_well_location_header h 
            ON h.well_location_application_number = a."WEL_LOC_CHA_DEEP_ID"
        JOIN operator_contracts_agreements.t_well_location_well_details w 
            ON w.well_name = a."WELL_NO" and  w.well_location_header_id = h.well_location_header_id
        JOIN dgh_staging.cms_file_ref cfr 
            ON cfr.ref_id IN (a."UPLOAD_OCR", a."WELL_TYPE_FILE")
        JOIN dgh_staging.cms_files cf 
            ON cf.file_id = cfr.file_id
        WHERE w.is_migrated = '1'
        
        union ALL
        
        SELECT a.refid ,cf.file_label ,cf.logical_doc_id ,cf.label_id, null as well_location_well_details_id ,b.well_location_header_id
        from dgh_staging.form_wel_loc_cha_deep a 
        join operator_contracts_agreements.t_well_location_header b on a.refid = b.well_location_application_number 
        join dgh_staging.cms_file_ref cfr on cfr.ref_id = a.fileref  
        join dgh_staging.cms_files cf on cf.file_id = cfr.file_id 
        where b.is_migrated = 1
    """)
    
    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} well location document records to insert")

    for wel_loc_cha_deep_id, file_label, logical_doc_id, label_id, well_details_id, header_id in rows:
        pg_cursor.execute("""
            INSERT INTO operator_contracts_agreements.t_well_location_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                well_location_well_details_id,
                well_location_header_id
            ) VALUES (%s, %s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_label, well_details_id, header_id))

        print(f"📥 Inserted: WELL_LOC_ID={wel_loc_cha_deep_id}, LogicalDocID={logical_doc_id}, "
              f"LabelID={label_id}, FileLabel={file_label}, WellDetailsID={well_details_id}, HeaderID={header_id}")
    
    pg_conn.commit()
    print("✅ All well location documents inserted successfully")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    print("🔚 PostgreSQL connection closed")
