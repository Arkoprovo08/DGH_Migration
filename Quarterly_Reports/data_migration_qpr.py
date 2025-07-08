import oracledb
import psycopg2
from datetime import datetime

# ACTIVITY_ID Mapping from 1.0 SEQ_NO to 2.0 IDs
SEQ_TO_ACTIVITY_ID = {
    2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 8, 10: 9, 11: 10,
    12: 11, 13: 12, 14: 13, 15: 14, 16: 15, 17: 16, 18: 17,
    22: 26, 23: 27, 24: 28, 25: 29, 26: 30, 27: 31, 28: 32, 29: 33,
    30: 34, 31: 35, 33: 18, 34: 19, 35: 20, 36: 21, 37: 22,
    38: 23, 39: 24, 40: 25, 41: 36, 42: 37, 43: 38
}

def map_seq_to_activity_id(seq_no):
    return SEQ_TO_ACTIVITY_ID.get(seq_no)

def get_pg_connection():
    return psycopg2.connect(
        host="3.110.185.154",
        port=5432,
        database="ims",
        user="postgres",
        password="P0$tgres@dgh"
    )

def get_oracle_connection():
    return oracledb.connect(
        user="PWCIMS",
        password="PWCIMS2025",
        dsn=oracledb.makedsn("192.168.0.133", 1521, sid="ORCL")
    )

def resolve_user(tgt_cursor, migrated_user_id):
    tgt_cursor.execute("""
        SELECT user_id FROM user_profile.m_user_master 
        WHERE is_migrated = 1 AND migrated_user_id = %s
    """, (migrated_user_id,))
    result = tgt_cursor.fetchone()
    return result[0] if result else None

def migrate_header(src_cursor, tgt_cursor):
    src_cursor.execute("""
        SELECT REFID, CONTRACTNAME, BLOCKCATEGORY, DOS_CONTRACT, BLOCKNAME,
               REF_TOPSC_ARTICALNO, PERIOD_OF_REPORTING, FINANCIAL_YEARS,
               CURRENCY, EXCHANGE_RATE, DATE_EXCHANGE_RATE,
               CREATED_BY, CREATED_ON, NAME_AUTH_SIG_CONTRA,
               DESIGNATION, COMMENTID, REVENUE_GOIPP_NA
        FROM FRAMEWORK01.FORM_PROGRESS_REPORT
        WHERE STATUS = 1
    """)
    records = src_cursor.fetchall()
    print(f"📦 Migrating {len(records)} QPR header records...")

    refid_to_qprid = {}

    for row in records:
        (
            refid, contractor_name, block_category, dos_contract, block_name,
            psc_article_no, qtr, year, currency, exch_rate, exch_date,
            created_by, created_on, auth_sig, designation, remarks, goipp
        ) = row

        user_id = resolve_user(tgt_cursor, created_by)
        if not user_id:
            print(f"⚠️ REFID {refid} skipped: No user mapping for {created_by}")
            continue

        insert_sql = """
            INSERT INTO upstream_data_management.t_quarterly_report_header (
                quarterly_report_application_number, contractor_name, block_category,
                date_of_contract_signature, block_name, ref_psc_article_no, period_of_reporting_quarter,
                period_of_reporting_year, period_of_reporting_currency, exchange_rate_considered,
                exchange_rate_date, created_by, creation_date, name_of_authorised_signatory,
                designation, remarks, is_goipp, process_id, is_active, is_migrated, current_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING quarterly_report_id
        """
        tgt_cursor.execute(insert_sql, (
            refid, contractor_name, block_category, dos_contract, block_name, psc_article_no,
            qtr, year, currency, exch_rate, exch_date, user_id, created_on, auth_sig,
            designation, remarks, goipp, 1, 1, 1, 'DRAFT'
        ))
        qpr_id = tgt_cursor.fetchone()[0]
        refid_to_qprid[refid] = qpr_id
        print(f"✅ Inserted header: REFID {refid} → QPR_ID {qpr_id}")

    return refid_to_qprid

def migrate_sections_by_activity(src_cursor, tgt_cursor, refid_to_qprid):
    src_cursor.execute("""
        SELECT REFID, SEQ_NO, WORK_PROG_QUT_ACT, PLANNED_QUT_ACT, ACTUAL_QUT_ACT,
               RE_FOR_VAR_QUT_ACT, CUMULATIVE_QUT_ACT, REMARK_QUT_ACT,
               WORK_PROG_QUT_EXP, PLANNED_QUT_EXP, ACTUAL_QUT_EXP,
               RE_FOR_VAR_QUT_EXP, CUMULATIVE_QUT_EXP, PER_ACH_APP_BUG_QUT_EXP,
               CREATED_BY, ACTIVE, LABEL
        FROM FRAMEWORK01.FORM_PROGRESS_REPORT_SEC
        WHERE STATUS = 1
    """)
    records = src_cursor.fetchall()
    print(f"📦 Migrating {len(records)} detail records based on ACTIVITY_ID...")

    for row in records:
        (
            refid, seq_no, wp_act_fy, qtr_plan_act, qtr_actual_act, var_act, cum_act, remarks_act,
            wp_exp_fy, qtr_plan_exp, qtr_actual_exp, var_exp, cum_exp, percent_ach,
            created_by, active, label
        ) = row

        qpr_id = refid_to_qprid.get(refid)
        if not qpr_id:
            print(f"⚠️ REFID {refid} skipped: No QPR_ID found.")
            continue

        user_id = resolve_user(tgt_cursor, created_by)
        if not user_id:
            print(f"⚠️ REFID {refid} skipped: No user mapping for {created_by}")
            continue

        activity_id = map_seq_to_activity_id(seq_no)
        if not activity_id:
            print(f"⚠️ REFID {refid} skipped: No mapping for SEQ_NO {seq_no}")
            continue

        # Decide the target table
        if 1 <= activity_id <= 17:
            table = "t_quarterly_report_exploration_activities"
        elif 18 <= activity_id <= 25:
            table = "t_quarterly_report_production_activities"
        elif 26 <= activity_id <= 35:
            table = "t_quarterly_report_developtment_activities"
        elif 36 <= activity_id <= 38:
            table = "t_quarterly_report_programme_quantity"
        else:
            print(f"⚠️ REFID {refid} skipped: Invalid activity_id {activity_id}")
            continue

        if table == "t_quarterly_report_programme_quantity":
            insert_sql = f"""
                INSERT INTO upstream_data_management.{table} (
                    quarterly_report_id, programme_quantity_name,
                    mc_approved_programme_quantity_fy, planned_production, actual_production,
                    reason_for_variance, cumulative_actual_production_eoq,
                    percentage_achieved, remarks, created_by, is_active, is_migrated, unit_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            tgt_cursor.execute(insert_sql, (
                qpr_id, label, wp_exp_fy, qtr_plan_exp, qtr_actual_exp,
                var_exp, cum_exp, percent_ach, remarks_act,
                user_id, active, 1, cum_act
            ))
        else:
            insert_sql = f"""
                INSERT INTO upstream_data_management.{table} (
                    quarterly_report_id, activity_id,
                    work_programme_mc_approved_activity_whole_fy,
                    quarterly_activity_planned, quarterly_activity_actual,
                    quarterly_activity_reason_for_variance, quarterly_activity_cumulative,
                    quarterly_activity_remarks, work_programme_mc_approved_budget_whole_fy,
                    quarterly_expenditure_planned, quarterly_expenditure_actual,
                    quarterly_expenditure_reason_for_variance, quarterly_expenditure_cumulative,
                    quarterly_expenditure_percentage_achieved, created_by, is_active, is_migrated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            tgt_cursor.execute(insert_sql, (
                qpr_id, activity_id, wp_act_fy, qtr_plan_act, qtr_actual_act, var_act, cum_act,
                remarks_act, wp_exp_fy, qtr_plan_exp, qtr_actual_exp, var_exp, cum_exp, percent_ach,
                user_id, active, 1
            ))

        print(f"✅ Inserted into {table} → REFID {refid}, ACTIVITY_ID {activity_id}")

def migrate_all():
    src_conn = get_oracle_connection()
    tgt_conn = get_pg_connection()
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()

    try:
        refid_to_qprid = migrate_header(src_cursor, tgt_cursor)
        tgt_conn.commit()

        migrate_sections_by_activity(src_cursor, tgt_cursor, refid_to_qprid)
        tgt_conn.commit()

    except Exception as e:
        print("❌ Migration error:", e)
        tgt_conn.rollback()
    finally:
        src_cursor.close()
        tgt_cursor.close()
        src_conn.close()
        tgt_conn.close()
        print("✅ All DB connections closed.")

if __name__ == "__main__":
    migrate_all()
