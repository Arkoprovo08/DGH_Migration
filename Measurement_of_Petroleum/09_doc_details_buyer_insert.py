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

    pg_cursor.execute("""
        SELECT 
            b.refid,
            cf.file_label,
            cf.logical_doc_id,
            cf.label_id,
            h.measurement_of_petroleum_id,
            ROW_NUMBER() OVER (PARTITION BY b.refid ORDER BY b.refid) as buyer_id
        FROM dgh_staging.form_measurement_petroleum b
        JOIN upstream_data_management.t_measurement_of_petroleum_header h 
            ON h.measurement_of_petroleum_application_number = b.refid
        JOIN dgh_staging.cms_file_ref cfr 
            ON cfr.ref_id = b.label_value
        JOIN dgh_staging.cms_files cf 
            ON cf.file_id = cfr.file_id
        WHERE b.label_text = 'Details of buyer (noofbuyers) to be included,(including Type of Meter, Make, Size, Range Percentage Error, frequency of calibration)' 
          AND b.status = '1'
          AND h.is_migrated = 1
        ORDER BY b.refid
    """)
    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} buyer details to insert")

    for refid, file_label, logical_doc_id, label_id, measurement_of_petroleum_id, buyer_id in rows:
        pg_cursor.execute("""
            INSERT INTO upstream_data_management.t_measurement_of_petroleum_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                measurement_of_petroleum_id,
                buyer_id
            ) VALUES (%s, %s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_label, measurement_of_petroleum_id, buyer_id))
        print(f"📥 Inserted: REFID={refid}, LogicalDocID={logical_doc_id}, LabelID={label_id}, FileLabel={file_label}, BuyerID={buyer_id}")
    pg_conn.commit()
    print("✅ All buyer details inserted successfully")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    print("🔚 PostgreSQL connection closed")