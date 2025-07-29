import psycopg2
from datetime import datetime

def parse_date_str(date_str):
    """Parses date string into yyyy-mm-dd format if valid"""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    return None

def transform_hydro_type(hydro_type):
    mapping = {
        "GAS_CONDENSATE": "Gas+Condensate",
        "OIL": "Oil",
        "OIL_GAS": "Oil+Gas",
        "GAS": "Gas"
    }
    return mapping.get(str(hydro_type).strip().upper(), hydro_type)

def migrate_format_c_all():
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
        # Step 1: Fetch data including remarks
        src_cursor.execute("""
            SELECT 
                f.CREATED_BY, 
                f.REFID, f.CONTRACTNAME, f.BLOCKNAME, f.DOS_CONTRACT, f.BLOCKCATEGORY, 
                f.UPLOAD_OCR, f.UPLOAD_OCR_NO_REMARK, f.NAME_OF_DISCOVERY, f.FORMAT_A_DATE, 
                f.FORMAT_B_DATE, f.HYDRO_TYPE, f.REF_TOPSC_ARTICALNO, f.CONTRACTOR_PARTIES, 
                f.AREA_BLOCK, f.NAME_OF_THE_FIELD, f.NUMBER_OF_HYDROCARBON, 
                f.NAME_AUTH_SIG_CONTRA, f.DESIGNATION, f.CREATED_ON, f.AVA_WITH_CONTRACTOR_CHK,
                c.comment_data
            FROM dgh_staging.FORM_COMMERICAL_DIS_FORMAT_C f
            LEFT JOIN dgh_staging.FRM_COMMENTS c ON f.COMMENTID = c.COMMENT_ID
            WHERE f.STATUS = '1'
        """)
        header_rows = src_cursor.fetchall()
        print(f"✅ Found {len(header_rows)} headers to migrate.")

        for row in header_rows:
            (
                source_created_by,
                refid, contractor_name, block_name, dos_contract, block_category,
                upload_ocr, upload_ocr_no_remark, name_of_discovery, format_a_date,
                format_b_date, hydro_type, ref_topsc_articalno, contractor_parties,
                area_block, name_of_field, number_of_hydrocarbon,
                name_auth_sig_contra, designation, created_on, ava_with_contractor_chk,
                comment_text
            ) = row

            # Normalize and parse fields
            format_a_date = parse_date_str(format_a_date)
            format_b_date = parse_date_str(format_b_date)
            hydro_type = transform_hydro_type(hydro_type)
            upload_ocr = 1 if str(upload_ocr).strip() == '1' else 0
            tick_a = 1 if str(ava_with_contractor_chk).strip() == '1' else 0
            tick_b = 1 if str(ava_with_contractor_chk).strip() == '2' else 0

            # Resolve created_by
            tgt_cursor.execute("""
                SELECT user_id 
                FROM user_profile.m_user_master 
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (source_created_by,))
            mapped_user = tgt_cursor.fetchone()
            created_by = mapped_user[0] if mapped_user else 5

            # Insert into 2.0 header table
            insert_header_sql = """
                INSERT INTO operator_contracts_agreements.t_format_c_commercial_discovery_header (
                    format_c_application_no, contractor_name, block_name, dos_contract,
                    block_category, ocr_available, justification_unavailability_ocr,
                    name_of_discoveries, date_of_discovery, date_of_notification,
                    hydrocarbon_type, ref_topsc_articalno, contractor_parties,
                    area_of_block, name_of_field, num_hydrocarbon_zones,
                    name_of_authorised_signatory, designation, creation_date,
                    tick_option_a, tick_option_b, process_id, created_by, is_active, is_migrated,
                    is_declared, declaration_checkbox, current_status, cluster_checkbox,
                    remarks
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, 14, %s, 1, 1, 1,
                        '{"acceptTerm1": true, "infaAcceptTerm1": true, "infaAcceptTerm2": true}',
                        'DRAFT', '{}', %s)
                RETURNING format_c_id
            """

            tgt_cursor.execute(insert_header_sql, (
                refid, contractor_name, block_name, dos_contract, block_category,
                upload_ocr, upload_ocr_no_remark, name_of_discovery, format_a_date,
                format_b_date, hydro_type, ref_topsc_articalno, contractor_parties,
                area_block, name_of_field, number_of_hydrocarbon,
                name_auth_sig_contra, designation, created_on,
                tick_a, tick_b, created_by, comment_text
            ))

            format_c_id = tgt_cursor.fetchone()[0]
            print(f"➡️ Migrated REFID={refid} → format_c_id={format_c_id}")

            # Insert well name if exists
            src_cursor.execute("""
                SELECT MAX(label_input)
                FROM dgh_staging.FORM_COMMERICAL_DIS_FORMAT_C2
                WHERE ACTIVE = '1' AND REFID = %s AND label_value = 'Well_Name'
            """, (refid,))
            well_name = src_cursor.fetchone()[0]

            if well_name:
                tgt_cursor.execute("""
                    INSERT INTO operator_contracts_agreements.t_format_c_well_details (
                        format_c_id, well_name_or_number, created_by, is_active, is_migrated
                    )
                    VALUES (%s, %s, %s, 1, 1)
                """, (format_c_id, well_name, created_by))
                print(f"   ✅ Well added for REFID={refid}: {well_name}")
            else:
                print(f"   ⚠️ No well name for REFID={refid}")

        conn.commit()
        print("🎉 FORMAT_C migration complete with remarks, hydro_type mapping, and well details.")

    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()

    finally:
        src_cursor.close()
        tgt_cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate_format_c_all()
