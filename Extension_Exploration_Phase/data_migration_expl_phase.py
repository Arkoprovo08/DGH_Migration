import oracledb
import psycopg2
from datetime import datetime

def to_bool_flag(val):
    return 1 if val and val.strip().upper() == 'YES' else 0

def parse_date(val):
    if not val:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None  # if parsing fails

def migrate_extension_exploration_phase():
    # Oracle DB connection
    src_conn = oracledb.connect(
        user="sys",
        password="pwc",
        dsn=oracledb.makedsn("localhost", 1521, sid="ORCL"),
        mode=oracledb.SYSDBA
    )
    src_cursor = src_conn.cursor()

    # PostgreSQL DB connection
    tgt_conn = psycopg2.connect(
        host="3.110.185.154",
        port=5432,
        database="ims",
        user="postgres",
        password="P0$tgres@dgh"
    )
    tgt_cursor = tgt_conn.cursor()

    try:
        # Step 1: Fetch main form data
        src_cursor.execute("""
            SELECT "REF_ID", "CONTRACTNAME", "BLOCKNAME", "BLOCKCATEGORY", "DOS_CONTRACT",
                   "BID_ROUND", "DATE_EFFECTIVE", "OCR_AVAIABLE", "OCR_UNAVAIABLE_TXT",
                   "REF_TOPSC_ARTICALNO", "NAME_AUTH_SIG_CONTRA", "DESIGNATION",
                   "IS_ACTIVE", "CREATED_BY", "CREATED_ON","CONSORTIUM"
            FROM dgh_staging.FORM_EXTENSION_EXPLO_PHASE a
            join dgh_staging.frm_workitem_master_new b on a."REF_ID" = b.ref_id
            WHERE "STATUS" = '1'
        """)
        rows = src_cursor.fetchall()
        print(f"✅ Found {len(rows)} main records.")

        for row in rows:
            (
                ref_id, contractor_name, block_name, block_category, dos_contract,
                bid_round, date_effective, ocr_available, ocr_unavail_text,
                psc_article_no, auth_sig_name, designation, is_active,
                created_by, created_on,consortium
            ) = row

            # Step 2: Fetch data fields
            src_cursor.execute("""
                SELECT "DATA_ID", "DATA_VALUE"
                FROM dgh_staging.FORM_EXTENSION_PHASE_DATA
                WHERE "REFID" = :refid
            """, {'refid': ref_id})
            data = dict(src_cursor.fetchall())

            # Step 3: Resolve user_id
            tgt_cursor.execute("""
                SELECT user_id FROM user_profile.m_user_master
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (created_by,))
            user = tgt_cursor.fetchone()
            if not user:
                print(f"⚠️ Skipping REF_ID {ref_id} — No user found for CREATED_BY = {created_by}")
                continue
            resolved_user_id = user[0]

            # Step 4: Prepare and execute insert
            insert_sql = """
                INSERT INTO operator_contracts_agreements.t_extension_exploration_phase_details (
                    extension_exploration_phase_details_applications_number,
                    process_id,
                    contractor_name, 
                    block_name, 
                    block_category,
                    dos_contract, 
                    awarded_under, 
                    effective_date, 
                    expiry_date,
                    extension_availed_past, 
                    ocr_available, 
                    ocr_unaivalibility_text,
                    psc_article_no, 
                    mwp_completed, 
                    extension_sought_mwp,
                    additional_work_prog_mc, 
                    additional_work_prog_mc_justification, --hi
                    additional_work_prog_mc_article, 
                    data_exploration_activities_not_submitted_reason,
                    ld_date_submitted, 
                    amt_of_ld_submitted_usd, 
                    cin_no,
                    bank_ref_no, 
                    crn, 
                    name_auth_sig_contra, 
                    designation,
                    is_active, 
                    created_by, 
                    creation_date, 
                    is_declared,
                    declaration_checkbox, 
                    is_migrated, 
                    current_status,
                    consortium
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            values = (
                ref_id,
                31,
                contractor_name,
                block_name,
                block_category,
                dos_contract,
                bid_round,
                date_effective,
                parse_date(data.get('txt_Expiry_Date')),
                to_bool_flag(data.get('ddl_Any_Extensions')),
                to_bool_flag(ocr_available),
                ocr_unavail_text,
                psc_article_no,
                to_bool_flag(data.get('ddl_MWP_PSC')),
                to_bool_flag(data.get('ddl_Extension_PSC')),
                to_bool_flag(data.get('ddl_Additional_Work_PSC')),
                data.get('txt_Justification'),
                data.get('txt_clause_PSC'),
                data.get('txt_Reason_data'),
                parse_date(data.get('txt_Date_LD')),
                data.get('txt_Amount_LD'),
                data.get('txt_CIN'),
                data.get('txt_Bank_Reference'),
                data.get('txt_CRN'),
                auth_sig_name,
                designation,
                is_active,
                resolved_user_id,
                created_on,
                1,
                '{}',
                1,
                'DRAFT',
                consortium
            )

            print(f"🔄 Inserting REF_ID {ref_id} ...")
            tgt_cursor.execute(insert_sql, values)
            tgt_conn.commit()
            print(f"✅ Inserted REF_ID {ref_id}")

    except Exception as e:
        print(f"❌ Error: {e}")
        tgt_conn.rollback()
    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()
        print("✅ All connections closed.")

if __name__ == "__main__":
    migrate_extension_exploration_phase()
