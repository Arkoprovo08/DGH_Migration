import psycopg2
from datetime import datetime

def parse_ddmmyyyy_to_date(date_str):
    """Parses DD/MM/YYYY string to datetime.date (YYYY-MM-DD format)"""
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    except:
        return None

def migrate_inventory_report():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="13.127.174.112",
        port=5432,
        database="ims",
        user="imsadmin",
        password="Dghims!2025"
    )
    src_cursor = conn.cursor()
    tgt_cursor = conn.cursor()

    try:
        # Step 1: Fetch source data
        src_cursor.execute("""
            SELECT "REFID", "LABEL_NO", "LABEL_TEXT", "LABEL_VALUE", "CREATED_BY", "CREATED_ON"
            FROM dgh_staging.FORM_INVENTORY_REPORT i
            JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = i."REFID"
            WHERE i."STATUS" = '1'
        """)
        rows = src_cursor.fetchall()
        print(f"✅ Found {len(rows)} rows.")

        pivot_data = {}
        meta = {}

        # Step 2: Pivot data by REFID
        for refid, label_no, label_text, label_value, created_by, created_on in rows:
            if refid not in pivot_data:
                pivot_data[refid] = {}
                meta[refid] = {'created_by': created_by, 'created_on': created_on}
            pivot_data[refid][label_no.strip()] = label_value

        print(f"🔄 Preparing to insert {len(pivot_data)} inventory reports...")

        # Step 3: Insert into final table
        for refid, fields in pivot_data.items():
            created_by = meta[refid]['created_by']
            created_on = meta[refid]['created_on']

            # Resolve user_id from migrated user mapping
            tgt_cursor.execute("""
                SELECT user_id FROM user_profile.m_user_master
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (created_by,))
            user_result = tgt_cursor.fetchone()
            if not user_result:
                print(f"⚠️ Skipping REFID {refid} → CREATED_BY {created_by} not mapped.")
                continue
            resolved_user_id = user_result[0]

            # Determine verification status
            verification_done = 0 if '6' in fields and fields['6'] else 1

            # Prepare SQL and values
            insert_sql = """
                INSERT INTO inventory_tracking.t_inventory_report (
                    inventory_report_application_number,
                    contractor_name, block_name,
                    date_of_contract_signature, block_category,
                    ref_psc_article_no, remarks, declaration_checkbox,
                    name_of_authorised_signatory, designation,
                    signature_or_digital_signature,
                    current_status, created_by, creation_date,
                    is_active, is_migrated, process_id, verfication_done,
                    dgh_representative_name, inventory_from_date,
                    inventory_to_date, last_date_physical_verification
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL,
                        'DRAFT', %s, %s, 1, 1, 17, %s, %s, %s, %s, %s)
            """

            values = (
                refid,
                fields.get('1'),  # contractor_name
                fields.get('2'),  # block_name
                parse_ddmmyyyy_to_date(fields.get('3')),  # date_of_contract_signature
                fields.get('4'),  # block_category
                fields.get('5'),  # ref_psc_article_no
                fields.get('102'),  # remarks
                '{"acceptTerm1": true}',  # declaration_checkbox
                fields.get('103'),  # name_of_authorised_signatory
                fields.get('104'),  # designation
                resolved_user_id,
                created_on,
                verification_done,
                fields.get('108'),  # dgh_representative_name
                parse_ddmmyyyy_to_date(fields.get('105')),  # inventory_from_date
                parse_ddmmyyyy_to_date(fields.get('106')),  # inventory_to_date
                parse_ddmmyyyy_to_date(fields.get('6'))     # last_date_physical_verification
            )

            print(f"📝 Inserting REFID {refid}...")
            tgt_cursor.execute(insert_sql, values)
            conn.commit()
            print(f"✅ Inserted REFID {refid}")

    except Exception as e:
        print(f"❌ Error during migration: {e}")
        conn.rollback()

    finally:
        src_cursor.close()
        tgt_cursor.close()
        conn.close()
        print("✅ All connections closed.")

if __name__ == "__main__":
    migrate_inventory_report()