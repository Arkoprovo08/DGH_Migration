import oracledb
import psycopg2
import re

def migrate_notice_next_phase():
    # ✅ Oracle connection
    src_conn = oracledb.connect(
        user="PWCIMS",
        password="PWCIMS2025",
        dsn=oracledb.makedsn("192.168.0.133", 1521, sid="ORCL")
    )
    src_cursor = src_conn.cursor()

    # ✅ Postgres connection
    tgt_conn = psycopg2.connect(
        host="13.127.174.112",
        port=5432,
        database="ims",
        user="imsadmin",
        password="Dghims!2025"
    )
    tgt_cursor = tgt_conn.cursor()

    try:
        # ✅ Fetch source data
        src_cursor.execute("""
            SELECT REFID, CONTRACTNAME, BLOCKNAME, DOS_CONTRACT, BLOCKCATEGORY, BID_ROUND, REF_TOPSC_ARTICALNO,
                   CE_PHASE, ED_CE_PHASE, OPTIONCHOSEN, NAME_AUTH_SIG_CONTRA, DESIGNATION, CREATED_BY, CREATED_ON,
                   ENTRY_NEXT_PHASE_FROM, ENTRY_NEXT_PHASE_TO, TXT_RELINQUISHMENT, MWP_COMPLETED, BG_COST_LD_MWP
            FROM FRAMEWORK01.FORM_NOTICE_NEXT_PHASE_RELIN
            WHERE STATUS = '1'
        """)
        rows = src_cursor.fetchall()
        print(f"✅ Found {len(rows)} rows to migrate.")

        for row in rows:
            (
                refid, contractname, blockname, dos_contract, blockcategory, bid_round, ref_topsc_articalno,
                ce_phase, ed_ce_phase, optionchosen, name_auth_sig_contra, designation, source_created_by, created_on,
                entry_next_phase_from, entry_next_phase_to, txt_relinquishment, mwp_completed, bg_cost_ld_mwp
            ) = row

            # ✅ Map option
            entry_option = None
            if optionchosen == '1':
                entry_option = 'Entry into Next Phase'
            elif optionchosen == '2':
                entry_option = 'Relinquishment'

            # ✅ Map CE Phase
            ce_phase_map = {
                '1': 'First', '2': 'Second', '3': 'Third',
                '4': 'Initial', '5': 'Subsequent'
            }
            ce_phase_label = ce_phase_map.get(str(ce_phase), None)

            # ✅ Clean percentage field
            clean_txt_relinquishment = None
            if txt_relinquishment:
                match = re.search(r'\d+(\.\d+)?', txt_relinquishment)
                clean_txt_relinquishment = match.group(0) if match else None

            # ✅ Map CREATED_BY
            tgt_cursor.execute("""
                SELECT user_id FROM user_profile.m_user_master
                WHERE is_migrated = 1 AND migrated_user_id = %s
            """, (source_created_by,))
            mapped_user = tgt_cursor.fetchone()
            created_by = mapped_user[0] if mapped_user else 5

            try:
                # ✅ Insert header with is_declared = 1
                tgt_cursor.execute("""
                    INSERT INTO operator_contracts_agreements.t_notice_entering_next_phase_relinquishment_header
                    (
                        entering_next_phase_relinquishment_application_number,
                        contractor_name, block_name, date_of_contract_signature,
                        block_category, awarded_under, reference_to_article_policy,
                        current_exploration_phase, expiry_date, entry_next_phase_relinquishment,
                        name_of_authorised_signatory, designation, created_by, creation_date,
                        is_active, process_id, current_status, is_migrated, is_declared
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 20, 'DRAFT', 1, 1)
                    RETURNING entering_next_phase_relinquishment_id
                """, (
                    refid, contractname, blockname, dos_contract,
                    blockcategory, bid_round, ref_topsc_articalno,
                    ce_phase_label, ed_ce_phase, entry_option,
                    name_auth_sig_contra, designation, created_by, created_on
                ))

                v_notice_primary_id = tgt_cursor.fetchone()[0]

                # ✅ Insert detail
                if entry_option == 'Entry into Next Phase':
                    tgt_cursor.execute("""
                        INSERT INTO operator_contracts_agreements.t_notice_entering_next_phase
                        (
                            entering_next_phase_relinquishment_id, next_phase_from_date, next_phase_to_date,
                            created_by, is_active, is_migrated
                        )
                        VALUES (%s, %s, %s, %s, 1, 1)
                    """, (
                        v_notice_primary_id, entry_next_phase_from, entry_next_phase_to, created_by
                    ))

                elif entry_option == 'Relinquishment':
                    tgt_cursor.execute("""
                        INSERT INTO operator_contracts_agreements.t_notice_relinquishment
                        (
                            entering_next_phase_relinquishment_id, percentage_of_area_under_relinquishment,
                            minimum_work_programme_completed, whether_submitted_bg_paid_cost_ld_against_unfinished_mwp,
                            created_by, is_active, is_migrated
                        )
                        VALUES (%s, %s, %s, %s, %s, 1, 1)
                    """, (
                        v_notice_primary_id, clean_txt_relinquishment, mwp_completed, bg_cost_ld_mwp, created_by
                    ))

                print(f"➡️ Migrated REFID={refid} as {entry_option}")

            except Exception as e:
                print(f"❌ Failed REFID={refid}: {e}")

        # ✅ Commit changes
        tgt_conn.commit()
        src_conn.commit()
        print("🎉 Migration completed successfully!")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        tgt_conn.rollback()
        src_conn.rollback()
    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()

if __name__ == "__main__":
    migrate_notice_next_phase()
