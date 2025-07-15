import oracledb
import psycopg2
from datetime import datetime

def migrate_inventory_report():
    # Oracle source connection
    src_conn = oracledb.connect(
        user="PWCIMS",
        password="PWCIMS2025",
        dsn=oracledb.makedsn("192.168.0.133", 1521, sid="ORCL")
    )
    src_cursor = src_conn.cursor()

    # PostgreSQL target connection
    tgt_conn = psycopg2.connect(
        host="3.110.185.154",
        port=5432,
        database="ims",
        user="postgres",
        password="P0$tgres@dgh"
    )
    tgt_cursor = tgt_conn.cursor()

    try:
        # Step 1: Fetch source data
        src_cursor.execute("""
            SELECT REFID, LABEL_NO, LABEL_TEXT, LABEL_VALUE, CREATED_BY, CREATED_ON
            FROM FRAMEWORK01.FORM_INVENTORY_REPORT
            WHERE STATUS = '1'
        """)

        rows = src_cursor.fetchall()
        print(f"✅ Found {len(rows)} rows.")

        pivot_data = {}
        meta = {}

        # Step 2: Pivot by REFID
        for refid, label_no, label_text, label_value, created_by, created_on in rows:
            if refid not in pivot_data:
                pivot_data[refid] = {}
                meta[refid] = {'created_by': created_by, 'created_on': created_on}
            pivot_data[refid][label_no.strip()] = label_value

        print(f"🔄 Preparing to insert {len(pivot_data)} reports.")

        # Step 3: Insert into target table
        for refid, fields in pivot_data.items():
            created_by = meta[refid]['created_by']
            created_on = meta[refid]['created_on']

            # Resolve actual user_id
            tgt_cursor.execute("""
                SELECT user_id 
                FROM user_profile.m_user_master 
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (created_by,))
            user_result = tgt_cursor.fetchone()
            if not user_result:
                print(f"⚠️ No user found for CREATED_BY = {created_by} ➜ skipping REFID {refid}")
                continue

            resolved_user_id = user_result[0]

            def parse_date(lbl):
                try:
                    return datetime.strptime(fields.get(lbl, ''), "%d/%m/%Y")
                except:
                    return None

            verification_done = 0 if '6' in fields and fields['6'] else 1

            insert_sql = """
                INSERT INTO inventory_tracking.t_inventory_report (
                    inventory_report_application_number, contractor_name, block_name,
                    date_of_contract_signature, block_category, ref_psc_article_no,
                    remarks, declaration_checkbox,
                    name_of_authorised_signatory, designation, signature_or_digital_signature,
                    current_status, created_by, creation_date, is_active,
                    is_migrated, process_id, verfication_done, dgh_representative_name,
                    inventory_from_date, inventory_to_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL,
                        'DRAFT', %s, %s, 1, 1, 17, %s, %s, %s, %s)
            """

            values = (
                refid,
                fields.get('1'),      # contractor_name
                fields.get('2'),      # block_name
                parse_date('3'),      # date_of_contract_signature
                fields.get('4'),      # block_category
                fields.get('5'),      # ref_psc_article_no
                fields.get('102'),    # remarks
                '{}',                 # declaration_checkbox (hardcoded)
                fields.get('103'),    # name_of_authorised_signatory
                fields.get('104'),    # designation
                resolved_user_id,     # created_by (resolved)
                created_on,           # creation_date
                verification_done,    # verfication_done
                fields.get('108'),    # dgh_representative_name
                parse_date('105'),    # inventory_from_date
                parse_date('106')     # inventory_to_date
            )

            print(f"📝 Inserting REFID {refid} ...")
            tgt_cursor.execute(insert_sql, values)
            tgt_conn.commit()
            print(f"✅ Inserted REFID {refid}")

    except Exception as ex:
        print(f"❌ Migration failed: {ex}")
        tgt_conn.rollback()

    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()
        print("✅ All connections closed.")

if __name__ == "__main__":
    migrate_inventory_report()
