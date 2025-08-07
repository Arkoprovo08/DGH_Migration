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
        WITH tank_details AS (
            SELECT 
                h.measurement_of_petroleum_application_number AS refid,
                t.measurement_of_petroleum_tank_details_id,
                t.type_of_tank,
                t.capacity,
                t.measurement_of_petroleum_id,
                ROW_NUMBER() OVER (
                    PARTITION BY t.measurement_of_petroleum_id 
                    ORDER BY t.measurement_of_petroleum_tank_details_id
                ) AS rn
            FROM upstream_data_management.t_measurement_of_petroleum_tank_details t
            JOIN upstream_data_management.t_measurement_of_petroleum_header h 
                ON h.measurement_of_petroleum_id = t.measurement_of_petroleum_id
            WHERE t.is_migrated = 1
        ),
        file_details AS (
            SELECT 
                b.refid,
                b.label_text,
                cf.file_name,
                cf.file_label,
                cf.logical_doc_id,
                cf.label_id,
                ROW_NUMBER() OVER (
                    PARTITION BY b.refid 
                    ORDER BY b.refid
                ) AS rn
            FROM dgh_staging.form_measurement_petroleum b
            JOIN upstream_data_management.t_measurement_of_petroleum_header h 
                ON h.measurement_of_petroleum_application_number = b.refid
            JOIN dgh_staging.cms_file_ref cfr ON cfr.ref_id = b.label_value
            JOIN dgh_staging.cms_files cf ON cf.file_id = cfr.file_id
            WHERE b.label_text = 'Frequency of Tank gauging'
            AND b.status = '1'
            AND h.is_migrated = 1
        )
        SELECT 
            t.refid ,
            f.file_label,
            f.logical_doc_id,
            f.label_id ,
            t.measurement_of_petroleum_id,
            t.measurement_of_petroleum_tank_details_id
        FROM tank_details t
        JOIN file_details f
            ON t.refid = f.refid
            AND t.rn = f.rn
        ORDER BY t.refid, t.measurement_of_petroleum_tank_details_id;
    """)
    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} documents to insert")

    for refid, file_name, logical_doc_id, label_id, measurement_of_petroleum_id,measurement_of_petroleum_tank_details_id in rows:
        pg_cursor.execute("""
            INSERT INTO upstream_data_management.t_measurement_of_petroleum_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                measurement_of_petroleum_id,
                measurement_of_petroleum_tank_details_id
            ) VALUES (%s, %s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_name, measurement_of_petroleum_id, measurement_of_petroleum_tank_details_id))
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