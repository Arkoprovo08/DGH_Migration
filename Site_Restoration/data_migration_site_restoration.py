import psycopg2
from datetime import datetime

def migrate_site_restoration():
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
        # Step 1: Insert base site restoration records
        src_cursor.execute("""
            SELECT a."REF_ID", a."CONTRACTNAME", a."BLOCKCATEGORY", a."DATE_EFFECTIVE", a."BLOCKNAME", 
                   a."REF_TOPSC_ARTICALNO", a."OCR_AVAIABLE", a."OCR_UNAVAIABLE_TXT",
                   a."CREATED_BY", a."CREATED_ON", a."BID_ROUND", a."DESIGNATION", 
                   a."DOS_CONTRACT", a."NAME_AUTH_SIG_CONTRA", fc.comment_data
            FROM dgh_staging.FORM_SITE_RESTORATION a
            JOIN dgh_staging.frm_workitem_master_new b 
                ON a."REF_ID" = b.ref_id
            LEFT JOIN dgh_staging.frm_comments fc 
                ON fc.comment_id = a."COMMENT_ID"
            WHERE a."STATUS" = '1' AND a."IS_ACTIVE" = '1'
        """)
        records = src_cursor.fetchall()
        print(f"✅ Step 1: Found {len(records)} records to migrate.")

        for row in records:
            (
                ref_id, contractor_name, block_category, eff_date_contract, block_name,
                psc_article_no, ocr_available, ocr_unavailability_text,
                source_created_by, creation_date, bid_round, designation,
                dos_contract, name_auth_signatory, comment_data
            ) = row

            # Resolve created_by from migrated_user_id
            tgt_cursor.execute("""
                SELECT user_id FROM user_profile.m_user_master 
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (source_created_by,))
            user_result = tgt_cursor.fetchone()

            if not user_result:
                print(f"⚠️ REF_ID {ref_id} skipped: No mapped user for source_created_by {source_created_by}")
                continue

            resolved_user_id = user_result[0]
            is_ocr_available = 1 if (ocr_available or '').strip().upper() == 'YES' else 0

            insert_sql = """
                INSERT INTO site_restoration.t_site_restoration_abandonment_details (
                    site_restoration_abandonment_application_number,
                    contractor_name, block_category, eff_date_contract, block_name,
                    psc_article_no, ocr_available, ocr_unaivailability_text, 
                    created_by, creation_date, awarded_under, designation, 
                    dos_contract, name_authorized_signatory, current_status, 
                    declaration_checkbox, process_id, is_active, is_migrated,
                    remarks
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            tgt_cursor.execute(insert_sql, (
                ref_id, contractor_name, block_category, eff_date_contract, block_name,
                psc_article_no, is_ocr_available, ocr_unavailability_text,
                resolved_user_id, creation_date, bid_round, designation,
                dos_contract, name_auth_signatory, 'DRAFT',
                '{}', 24, 1, 1, comment_data
            ))

            print(f"✅ Inserted REF_ID {ref_id}")
            conn.commit()

        # Step 2: Update detailed fields
        src_cursor.execute("""
            SELECT DISTINCT a."REFID"
            FROM dgh_staging.FORM_SITE_RESTORATION_DATA a
            JOIN dgh_staging.FORM_SITE_RESTORATION b 
                ON a."REFID" = b."REF_ID"
            WHERE a."STATUS" = '1'
        """)
        detail_refs = src_cursor.fetchall()
        print(f"✅ Step 2: Found {len(detail_refs)} detail records to update.")

        for (refid,) in detail_refs:
            src_cursor.execute("""
                SELECT "DATA_ID", "DATA_VALUE"
                FROM dgh_staging.FORM_SITE_RESTORATION_DATA a
                JOIN dgh_staging.FORM_SITE_RESTORATION b ON a."REFID" = b."REF_ID"
                WHERE a."REFID" = %s
            """, (refid,))
            data = {row[0]: row[1] for row in src_cursor.fetchall()}

            def get_val(k): return data.get(k)
            def parse_date(d): 
                try: return datetime.strptime(d, '%d/%m/%Y') if d else None
                except: return None
            def parse_number(n): 
                try: return float(n) if n else None
                except: return None
            def parse_bool(v): return 1 if (v or '').strip().upper() == 'YES' else 0

            v_srf_1999 = get_val('DDL_SRF_1999')
            v_srf_2018 = get_val('DDL_SRF_2018')
            v_srf_2021 = get_val('DDL_SRF_2021')
            v_profit_petroleum = get_val('DDL_Profit_Petroleum')
            v_bank_guarantee = get_val('ddl_Bank_Guarantee')
            v_bg_from_date = parse_date(get_val('txt_Bank_Guarantee_from_date'))
            v_bg_to_date = parse_date(get_val('txt_Bank_Guarantee_to_date'))
            v_bg_date = get_val('txt_Bank_Guarantee_date')
            v_bg_amt = parse_number(get_val('txt_Bank_Guarantee_amt'))
            v_details_calc = get_val('ddl_details_calculation')

            is_calc_provided = parse_bool(v_details_calc)
            is_compl_psc = parse_bool(v_profit_petroleum)

            update_sql = """
                UPDATE site_restoration.t_site_restoration_abandonment_details
                SET srf_1999_value = %s,
                    srg_2018_value = %s,
                    srg_2021_value = %s,
                    compliance_psc = %s,
                    date_amt_period_bg = %s,
                    yes_period_from = %s,
                    yes_period_to = %s,
                    date_input = %s,
                    amount_usd = %s,
                    is_calculation_of_bg = %s
                WHERE site_restoration_abandonment_application_number = %s
            """

            tgt_cursor.execute(update_sql, (
                v_srf_1999, v_srf_2018, v_srf_2021, is_compl_psc,
                v_bank_guarantee, v_bg_from_date, v_bg_to_date,
                v_bg_date, v_bg_amt, is_calc_provided, refid
            ))

            conn.commit()
            print(f"🔄 Updated REF_ID {refid}")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        conn.rollback()
    finally:
        src_cursor.close()
        tgt_cursor.close()
        conn.close()
        print("✅ PostgreSQL connection closed.")

if __name__ == "__main__":
    migrate_site_restoration()
