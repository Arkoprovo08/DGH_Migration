import oracledb
import psycopg2
from datetime import datetime

def migrate_liquidated_damages():
    # Connect to Oracle (Source)
    src_conn = oracledb.connect(
        user="PWCIMS",
        password="PWCIMS2025",
        dsn=oracledb.makedsn("192.168.0.133", 1521, sid="ORCL")
    )
    src_cursor = src_conn.cursor()

    # Connect to Postgres (Target)
    tgt_conn = psycopg2.connect(
        host="3.110.185.154",
        port=5432,
        database="ims",
        user="postgres",
        password="P0$tgres@dgh"
    )
    tgt_cursor = tgt_conn.cursor()

    try:
        # Step 1: Fetch base rows from Oracle
        src_cursor.execute("""
            SELECT 
                REF_ID, BLOCKNAME, CONTRACTNAME, BLOCKCATEGORY, DOS_CONTRACT,
                DATE_EFFECTIVE, REF_TOPSC_ARTICALNO, NAME_AUTH_SIG_CONTRA,
                DESIGNATION, IS_ACTIVE, CREATED_ON, BID_ROUND, CREATED_BY
            FROM FRAMEWORK01.FORM_LD_CALCULATIONS
            WHERE STATUS = '1' AND SEQ = 1
        """)
        rows = src_cursor.fetchall()
        print(f"✅ Found {len(rows)} records for LD Step 1 migration.")

        for row in rows:
            (
                ref_id, blockname, contractname, blockcategory, dos_contract,
                date_effective, ref_topsc_articalno, name_auth_sig_contra,
                designation, is_active, created_on, bid_round, source_created_by
            ) = row

            # Resolve created_by from migrated_user_id
            tgt_cursor.execute("""
                SELECT user_id 
                FROM user_profile.m_user_master 
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (source_created_by,))
            user_row = tgt_cursor.fetchone()

            if not user_row:
                print(f"⚠️ Skipping REF_ID {ref_id} due to unmapped CREATED_BY: {source_created_by}")
                continue

            resolved_user_id = user_row[0]

            # Insert into Postgres
            tgt_cursor.execute("""
                INSERT INTO financial_mgmt.t_liquidated_damages_details
                (liquidated_damages_application_number, block_name, contractor_name,
                 block_category, date_of_signing_contract, effective_date, article_number,
                 name_authorized_signatory, designation, is_active, process_id, created_by,
                 creation_date, awarded_under, is_migrated, current_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                ref_id, blockname, contractname, blockcategory, dos_contract,
                date_effective, ref_topsc_articalno, name_auth_sig_contra,
                designation, is_active, 32, resolved_user_id,
                created_on, bid_round, 1, 'DRAFT'
            ))
            tgt_conn.commit()
            print(f"✅ Inserted REF_ID {ref_id}")

            # Step 2: Fetch and update extra details from FORM_LD_CALCULATIONS_DATA
            src_cursor.execute("""
                SELECT data_id, data_value 
                FROM FRAMEWORK01.FORM_LD_CALCULATIONS_DATA
                WHERE refid = :refid
            """, {'refid': ref_id})
            data_rows = src_cursor.fetchall()
            data_map = {row[0]: row[1] for row in data_rows}

            def get_val(key, default=None):
                return data_map.get(key, default)

            def parse_date(val):
                try:
                    return datetime.strptime(val, "%d/%m/%Y") if val else None
                except:
                    return None

            def parse_number(val):
                try:
                    return float(val) if val else None
                except:
                    return None

            update_query = """
                UPDATE financial_mgmt.t_liquidated_damages_details
                SET minimum_work_programme_text = %s,
                    mandatory_work_programme_text = %s,
                    exploration_phase_dropdown = %s,
                    date_end_exploration_phase = %s,
                    mwp_completed = %s,
                    shortfall_target_depth_text = %s,
                    shortfall_geological_objective_text = %s,
                    protagnostic_thickness_text = %s,
                    possibility_potential_reservoir = %s,
                    ep_data_submitted = %s,
                    date_coumwp = %s,
                    amount_coumwp_paid_usd = %s,
                    head_account = %s,
                    cin_no = %s,
                    bank_ref_no = %s,
                    crn = %s,
                    is_detailed_calculations_provided = %s,
                    compl_psc_terms = %s,
                    amount_coumwp_paid_inr = %s
                WHERE liquidated_damages_application_number = %s
            """
            update_values = (
                get_val('txt_Unfinished_Minimum'),
                get_val('txt_Unfinished_Mandatory'),
                get_val('txt_unfinished_quantum'),
                parse_date(get_val('txt_Date_Exploration_Phase')),
                get_val('txt_concerned_phase'),
                get_val('txt_target_depth'),
                get_val('txt_Shortfall_achieving'),
                get_val('txt_Prognosticated_thickness'),
                get_val('txt_Possibility_potential'),
                get_val('ddl_EP_Data'),
                parse_date(get_val('txt_Date_COUMWP')),
                parse_number(get_val('txt_Amount_COUMWP_USD')),
                get_val('txt_Head_Account'),
                get_val('txt_CIN'),
                get_val('txt_Bank_Reference'),
                get_val('txt_CRN'),
                get_val('ddl_Agreement_between_operators'),
                get_val('ddl_Compliance_PSC'),
                parse_number(get_val('txt_Amount_COUMWP_INR')),
                ref_id
            )

            tgt_cursor.execute(update_query, update_values)
            tgt_conn.commit()
            print(f"🔄 Updated details for REF_ID {ref_id}")

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
    migrate_liquidated_damages()
