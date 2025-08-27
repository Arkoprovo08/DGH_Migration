import psycopg2

# === Connect to PostgreSQL ===
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

    # === Step 1: Fetch CMS + Header Data ===
    pg_cursor.execute("""
        SELECT cmf.refid, cf.file_label, cf.logical_doc_id, cf.label_id,
               taad.bank_gurantee_header_id
        FROM dgh_staging.cms_files cf
        JOIN dgh_staging.cms_file_ref cfr ON cf.file_id = cfr.file_id
        JOIN dgh_staging.cms_master_fileref cmf ON cfr.ref_id = cmf.fileref
        JOIN financial_mgmt.t_bank_gurantee_header taad 
            ON taad.bank_gurantee_application_number = cmf.refid
        WHERE taad.is_migrated = 1 AND cf.is_active = 1
    """)

    rows = pg_cursor.fetchall()
    print(f"🔍 Found {len(rows)} CMS rows")

    # === Step 2: Loop over rows ===
    for refid, file_name, logical_doc_id, label_id, header_id in rows:
        print(f"➡️ REFID={refid} ➜ HEADER_ID={header_id}")

        # === Step 3: Fetch consortium from Postgres instead of Oracle ===
        pg_cursor.execute("""
            SELECT "SEQ"
            FROM dgh_staging.form_sub_bg_legal_renewal
            WHERE "REFID" = %s AND "STATUS" = '1'
        """, (refid,))
        result = pg_cursor.fetchone()

        if result and result[0]:
            consortium = result[0]
            consortium_parts = [p.strip() for p in consortium.split(",")]
        else:
            consortium_parts = [None]

        # === Step 4: Process contractors ===
        for part in consortium_parts:
            if part:
                contractor_name = part.split("(")[0].strip()
            else:
                contractor_name = None

            if not contractor_name:
                print(f"⚠️ No contractor for REFID={refid}")
                continue

            # === Step 5: Find detail_id ===
            pg_cursor.execute("""
                SELECT bank_gurantee_details_id
                FROM financial_mgmt.t_bank_gurantee_details
                WHERE bank_gurantee_header_id = %s AND contractor_name = %s
            """, (header_id, contractor_name))
            detail_rows = pg_cursor.fetchall()

            if not detail_rows:
                print(f"❌ No details_id for contractor '{contractor_name}' in header_id {header_id}")
                continue

            # === Step 6: Insert document details ===
            for (details_id,) in detail_rows:
                pg_cursor.execute("""
                    INSERT INTO financial_mgmt.t_bank_gurantee_document_details
                    (
                        bank_gurantee_details_id, 
                        document_ref_number,
                        document_type_id,
                        document_name
                    )
                    VALUES (%s, %s, %s, %s)
                """, (
                    details_id,
                    logical_doc_id,
                    label_id,
                    file_name
                ))

                print(f"✅ Inserted ➜ REFID={refid} Contractor={contractor_name} DetailsID={details_id}")

    # === Commit changes ===
    pg_conn.commit()
    print("✅ All document rows inserted successfully")

except Exception as e:
    print(f"❌ Error: {e}")
    pg_conn.rollback()

finally:
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    print("🔚 PostgreSQL connection closed")
