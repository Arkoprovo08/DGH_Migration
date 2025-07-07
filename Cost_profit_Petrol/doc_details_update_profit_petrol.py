import psycopg2

# PostgreSQL DB Connection
pg_conn = psycopg2.connect(
    host="3.110.185.154",
    port=5432,
    database="ims",
    user="postgres",
    password="P0$tgres@dgh"
)

try:
    pg_cursor = pg_conn.cursor()
    print("✅ Connected to PostgreSQL")

    # Execute the query directly from PostgreSQL
    pg_cursor.execute("""
        SELECT cmf.refid, cf.file_name, cf.logical_doc_id, cf.label_id,
               taad.quarterly_report_cost_and_profit_petroleum_calculations_id
        FROM dgh_staging.cms_files cf
        JOIN dgh_staging.cms_file_ref cfr ON cf.FILE_ID = cfr.FILE_ID
        JOIN dgh_staging.cms_master_fileref cmf ON cfr.ref_id = cmf.fileref
        JOIN financial_mgmt.t_cost_and_profit_petroleum_calculations  taad 
            ON taad.quarterly_report_cost_and_profit_petroleum_calculations_applica   = cmf.refid
        WHERE taad.is_migrated = 1 AND cf.is_active = 1
    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} documents to insert")

    for refid, file_name, logical_doc_id, label_id, appointment_auditor_id in rows:
        pg_cursor.execute("""
            INSERT INTO financial_mgmt.t_cost_and_profit_petroleum_calculations_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                appointment_auditor_id
            ) VALUES (%s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_name, appointment_auditor_id))

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
