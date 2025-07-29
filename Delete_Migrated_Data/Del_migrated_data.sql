DO $$
DECLARE
    rec RECORD;
    del_sql TEXT;
BEGIN
    FOR rec IN
        WITH RECURSIVE fk_chain AS (
            -- Base: tables with is_migrated
            SELECT
                n.oid::regnamespace::text COLLATE "C" AS schema_name,
                c.oid::regclass::text COLLATE "C" AS table_name,
                c.oid AS table_oid,
                1 AS level,
                NULL::oid AS parent_oid,
                NULL::text COLLATE "C" AS fk_column,
                NULL::text COLLATE "C" AS pk_column
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE a.attname = 'is_migrated'
              AND c.relkind = 'r' 
			  and c.oid::regclass::text = 'financial_mgmt.t_bank_gurantee_header'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema','user_profile','activiti_workflow_query_audit','global_master')
 
            UNION ALL
 
            -- Recursive: find child tables
            SELECT
                child_ns.nspname::text COLLATE "C" AS schema_name,
                child_cls.relname::text COLLATE "C" AS table_name,
                child_cls.oid AS table_oid,
                fc.level + 1,
                con.confrelid AS parent_oid,
                child_att.attname::text COLLATE "C" AS fk_column,
                parent_att.attname::text COLLATE "C" AS pk_column
            FROM pg_constraint con
            JOIN fk_chain fc ON con.confrelid = fc.table_oid
            JOIN pg_class child_cls ON child_cls.oid = con.conrelid
            JOIN pg_namespace child_ns ON child_ns.oid = child_cls.relnamespace
            JOIN pg_attribute child_att ON child_att.attrelid = con.conrelid AND child_att.attnum = ANY(con.conkey)
            JOIN pg_attribute parent_att ON parent_att.attrelid = con.confrelid AND parent_att.attnum = ANY(con.confkey)
            WHERE con.contype = 'f'
              AND child_cls.relkind = 'r'
        )
        SELECT DISTINCT * FROM fk_chain
        ORDER BY level DESC
    LOOP
        BEGIN
            IF rec.parent_oid IS NOT NULL THEN
                -- Child table: delete via FK to parent with is_migrated = 1
                del_sql := format(
                    'DELETE FROM %I.%I c WHERE EXISTS (
                        SELECT 1 FROM %I.%I p WHERE p.is_migrated = 1 AND c.%I = p.%I
                    );',
                    rec.schema_name, rec.table_name,
                    (SELECT schema_name FROM fk_chain WHERE table_oid = rec.parent_oid LIMIT 1),
                    (SELECT table_name FROM fk_chain WHERE table_oid = rec.parent_oid LIMIT 1),
                    rec.fk_column, rec.pk_column
                );
            ELSE
                -- Root table: directly has is_migrated
                del_sql := format(
                    'DELETE FROM %I.%I WHERE is_migrated = 1;',
                    rec.schema_name, rec.table_name
                );
            END IF;
 
            EXECUTE del_sql;
            RAISE NOTICE 'Deleted from %', rec.schema_name || '.' || rec.table_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Skipped % due to error: %',
                         rec.schema_name || '.' || rec.table_name, SQLERRM;
        END;
    END LOOP;
END $$;
