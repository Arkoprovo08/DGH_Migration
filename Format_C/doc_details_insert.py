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
        SELECT 
            f.format_c_application_no,
            cf.file_label,
            cf.logical_doc_id,
            cf.label_id,
            w.format_c_id,
            w.format_c_well_details_id
        FROM dgh_staging.form_commerical_dis_format_c2 fc
        JOIN operator_contracts_agreements.t_format_c_commercial_discovery_header f
            ON fc.refid = f.format_c_application_no
        LEFT JOIN operator_contracts_agreements.t_format_c_well_details w 
            ON w.format_c_id = f.format_c_id
        JOIN dgh_staging.cms_file_ref cfr 
            ON cfr.ref_id = fc.label_input
        JOIN dgh_staging.cms_files cf 
            ON cf.file_id = cfr.file_id 
        WHERE fc.label_value IN (
            SELECT DISTINCT label_value
            FROM dgh_staging.form_commerical_dis_format_c2 fcfs 
            WHERE LABEL_TYPE = 'upload'
        )
    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} Format C document records to insert")

    for app_no, file_label, logical_doc_id, label_id, format_c_id, format_c_well_details_id in rows:
        pg_cursor.execute("""
            INSERT INTO operator_contracts_agreements.t_format_c_commercial_discovery_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                format_c_id,
                format_c_well_details_id
            ) VALUES (%s, %s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_label, format_c_id, format_c_well_details_id))

        print(f"📥 Inserted: AppNo={app_no}, LogicalDocID={logical_doc_id}, LabelID={label_id}, "
              f"FileLabel={file_label}, FormatC_ID={format_c_id}, FormatC_Well_ID={format_c_well_details_id}")
    
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
