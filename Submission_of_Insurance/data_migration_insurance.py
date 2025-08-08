import oracledb
import psycopg2

def migrate_insurance_indemnity():
    # 1️⃣ Connect to Oracle (source)
    src_conn = oracledb.connect(
        user="PWCIMS",
        password="PWCIMS2025",
        dsn=oracledb.makedsn("192.168.0.133", 1521, sid="ORCL")
    )
    src_cursor = src_conn.cursor()

    # 2️⃣ Connect to Postgres (target)
    tgt_conn = psycopg2.connect(
        host="13.127.174.112",
        port=5432,
        database="ims",
        user="imsadmin",
        password="Dghims!2025"
    )
    tgt_cursor = tgt_conn.cursor()

    try:
        # 3️⃣ Fetch rows to migrate
        src_cursor.execute("""
            SELECT 
                s."REFID", s."CONTRACTNAME", s."BLOCKNAME", s."DOS_CONTRACT", s."BLOCKCATEGORY",
                s."REF_TOPSC_ARTICALNO", s."NAME_OF_INSURANCE_COMPANY", s."SUM_INSURED_USD",
                s."SUM_INSURED", s."ASS_VAL_COVERED", s."TERM_OF_INSURANCE", s."TERM_OF_INSURANCE_TO",
                s."NAME_AUTH_SIG_CONTRA", s."DESIGNATION", s."CREATED_BY", s."CREATED_ON",
                fc.comment_data AS remarks
            FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY s
            join dgh_staging.frm_workitem_master_new w on w.ref_id = s."REFID" 
            LEFT JOIN LATERAL (
                SELECT comment_data
                FROM dgh_staging.frm_comments
                WHERE comment_id = s."COMMENTID"
                LIMIT 1
            ) fc ON TRUE
            WHERE s."STATUS" = '1';
        """)

        rows = src_cursor.fetchall()
        print(f"✅ Total rows fetched: {len(rows)}")

        for row in rows:
            (
                refid, contractor_name, block_name, date_of_contract_signature, block_category,
                ref_psc_article_no, insurance_company_name, sum_insured_usd,
                sum_insured_inr, asset_value_covered_in_usd, insurance_from, insurance_to,
                name_of_authorised_signatory, designation, source_created_by, creation_date, remarks
            ) = row

            tgt_cursor.execute("""
                SELECT user_id 
                FROM user_profile.m_user_master 
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (source_created_by,))
            user_id_result = tgt_cursor.fetchone()
            resolved_user_id = user_id_result[0] if user_id_result else None

            if resolved_user_id is None:
                print(f"⚠️ No user found for CREATED_BY = {source_created_by} ➜ inserting NULL for created_by")

            insert_sql = """
                INSERT INTO financial_mgmt.t_submission_of_insurance_and_indemnity
                (
                    insurance_and_indemnity_application_number,
                    contractor_name,
                    block_name,
                    date_of_contract_signature,
                    block_category,
                    ref_psc_article_no,
                    insurance_company_name,
                    sum_insured_in_usd,
                    sum_insured_in_inr,
                    asset_value_covered_in_usd,
                    insurance_from,
                    insurance_to,
                    name_of_authorised_signatory,
                    designation,
                    created_by,
                    creation_date,
                    is_active,
                    current_status,
                    process_id,
                    declaration_checkbox,
                    is_migrated,
                    remarks
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            insert_values = (
                refid,
                contractor_name,
                block_name,
                date_of_contract_signature,
                block_category,
                ref_psc_article_no,
                insurance_company_name,
                sum_insured_usd,
                sum_insured_inr,
                asset_value_covered_in_usd,
                insurance_from,
                insurance_to,
                name_of_authorised_signatory,
                designation,
                resolved_user_id,
                creation_date,
                1,             # is_active
                'DRAFT',       # current_status
                23,            # process_id
                '{"acceptTerm1": true}',          # declaration_checkbox
                1,              # is_migrated
                remarks
            )

            tgt_cursor.execute(insert_sql, insert_values)

        tgt_conn.commit()
        print(f"✅ Migrated {len(rows)} rows to t_submission_of_insurance_and_indemnity")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        tgt_conn.rollback()
    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()
        print("✅ All connections closed.")

if __name__ == "__main__":
    migrate_insurance_indemnity()
