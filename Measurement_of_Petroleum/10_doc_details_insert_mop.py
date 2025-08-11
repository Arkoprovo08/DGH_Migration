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
        select 
            fmp.refid,
            cf.file_label,
            cf.logical_doc_id,
            cf.label_id,
			tmoph.measurement_of_petroleum_id
        from dgh_staging.form_measurement_petroleum fmp  
        join dgh_staging.cms_file_ref cfr on fmp.label_value = cfr.ref_id 
        join dgh_staging.cms_files cf on cf.file_id = cfr.file_id
        join upstream_data_management.t_measurement_of_petroleum_header tmoph 
        on tmoph.measurement_of_petroleum_application_number = fmp.refid 
        where fmp.LABEL_TYPE = 'upload' 
        and fmp.label_text in (
        'Additional Files if any',
		'Allocation method',
		'Attach PFD and mention number points where Petroleum measurement to be done with in contract boundary',
		'Complete flow diagram of production facility with measurement points and storage points starting from well heads to delivery points ',
		'Details of API/AGA/ASTM/ISO Standards followed for Measurement System Design.',
		'Details of buyer (noofbuyers) to be included,(including Type of Meter, Make, Size, Range Percentage Error, frequency of calibration)',
		'Details of meters installed at site for measurement of fluids with model, make, specifications and OEM details.',
		'Dip tape detail',
		'Frequency of Tank gauging',
		'Method to be employed for measurement of volume of Petroleum production',
		'Operator to provide recommended frequency of inspection as per general standard of each meter and custody transfer meter',
		'Provide procedure of calibration/testing for each measurement device used in MOP exercise.',
		'Schematics related to metering skids, storage tanks or Measurement of Petroleum etc.',
		'Submit COSA and GSA signed with each party',
		'The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the limits of this contract.',
		'The point or points at which petroleum shall be measured and the respective shares allocated to the parties in accordance with the terms of this contract.',
		'Upload OCR ',
		'As built drawing related to metering skids, storage tanks or Measurement of Petroleum etc.',
		'Name of third party and Year of experience',
		'UPLOAD OVERVIEW OF BLOCK'
        )
        and fmp.status = '1' and tmoph.is_migrated = 1
    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} documents to insert")

    for refid, file_name, logical_doc_id, label_id, measurement_of_petroleum_id in rows:
        pg_cursor.execute("""
            INSERT INTO upstream_data_management.t_measurement_of_petroleum_document_details (
                document_ref_number,
                document_type_id,
                document_name,
                measurement_of_petroleum_id
            ) VALUES (%s, %s, %s, %s)
        """, (logical_doc_id, label_id, file_name, measurement_of_petroleum_id))

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
