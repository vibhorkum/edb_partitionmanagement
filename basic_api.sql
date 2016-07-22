CREATE SCHEMA IF NOT EXISTS edb_ddl_api;

CREATE OR REPLACE FUNCTION edb_ddl_api.get_table_columns(table_oid OID)
RETURNS TABLE(col_seq smallint,col_details TEXT)
LANGUAGE sql
AS
$function$
   SELECT   a.attnum, 
             ' ' 
                      || pg_catalog.quote_ident(a.attname) 
                      || ' ' 
                      || pg_catalog.format_type ( a.atttypid, a.atttypmod ) || 
             ( 
                    SELECT 
                           CASE 
                                  WHEN substring ( pg_catalog.pg_get_expr ( d.adbin, d.adrelid ) FOR 128 ) IS NULL THEN ''
                                  ELSE ' DEFAULT ' 
                                                || substring ( pg_catalog.pg_get_expr ( d.adbin, d.adrelid ) FOR 128 )
                           END 
                    FROM   pg_catalog.pg_attrdef d 
                    WHERE  d.adrelid = a.attrelid 
                    AND    d.adnum = a.attnum 
                    AND    a.atthasdef ) || 
             CASE 
                      WHEN a.attnotnull THEN ' NOT NULL ' 
                      ELSE '' 
             END ||
             ( 
                    SELECT 
                           CASE 
                                  WHEN c.collname IS NULL THEN '' 
                                  ELSE ' COLLATE ' 
                                                || c.collname 
                                                || ' ' 
                           END 
                    FROM   pg_catalog.pg_collation c, 
                           pg_catalog.pg_type t 
                    WHERE  c.oid = a.attcollation 
                    AND    t.oid = a.atttypid 
                    AND    a.attcollation <> t.typcollation ) AS column
    FROM     pg_catalog.pg_attribute a 
    WHERE    a.attrelid = $1
    AND      a.attnum > 0 
    AND      NOT a.attisdropped 
$function$;

CREATE OR REPLACE FUNCTION edb_ddl_api.get_table_ddl(table_oid OID,
                                                   on_tblspace BOOLEAN DEFAULT FALSE)
RETURNS TEXT
LANGUAGE sql
AS 
$function$
SELECT
    CASE
        WHEN d.relpersistence = 'u' THEN 'CREATE UNLOGGED TABLE ' || pg_catalog.quote_ident (
            d.relname )
        || ' ('
        WHEN d.relpersistence = 't' THEN 'CREATE TEMP TABLE ' || pg_catalog.quote_ident (
            d.relname )
        || ' ('
        ELSE 'CREATE TABLE ' || pg_catalog.quote_ident (
            d.relname )
        || ' ('
    END || pg_catalog.replace (
        d.columns,
        ',',
        E',\n' )
    || CASE
        WHEN d.constraint_def <> ''
        OR d.constraint_def IS NOT NULL THEN E',\n' || d.constraint_def
        ELSE ''
    END || ') ' || CASE
        WHEN d.reloptions <> '' THEN 'WITH (' || d.reloptions || ') '
        ELSE ''
    END || CASE
        WHEN d.relhasoids THEN 'WITH OIDS '
        ELSE ''
    END || CASE
        WHEN d.tablespace IS NOT NULL THEN d.tablespace
        ELSE ''
    END || CASE
        WHEN d.part_details IS NOT NULL THEN E'\n' || d.part_details
        ELSE ''
    END || ';'
FROM (
        SELECT
            c.relname,
            c.relpersistence,
            c.relkind,
            pg_catalog.array_to_string (
                (
                    SELECT
                        pg_catalog.array_agg (
                            col_details )
                    FROM
                        edb_ddl_api.get_table_columns (
                            c.oid ) ),
                ',' ) AS COLUMNS,
            pg_catalog.array_to_string (
                c.reloptions || ARRAY (
                    SELECT
                        'toast.' || x
                    FROM
                        pg_catalog.unnest (
                            tc.reloptions )
                        x ),
                ', ' ) AS reloptions,
            c.relhasoids,
            CASE
                WHEN c.reltablespace <> 0
                AND $2 THEN (
                    SELECT
                        'TABLESPACE ' || pg_catalog.quote_ident (
                            spcname )
                        || ' '
                    FROM
                        pg_catalog.pg_tablespace t
                    WHERE
                        t.oid = c.reltablespace )
            END AS TABLESPACE,
            CASE
                WHEN (
                    SELECT
                        count (
                            1 )
                    FROM
                        pg_catalog.edb_partdef
                    WHERE
                        pdefrel = c.oid )
                >= 1 THEN pg_catalog.pg_get_partdef (
                    c.oid )
            END AS part_details,
            (
                SELECT
                    pg_catalog.array_to_string (
                        pg_catalog.array_agg (
                            ' CONSTRAINT ' || pg_catalog.quote_ident (
                                c.conname )
                            || ' ' || pg_catalog.pg_get_constraintdef (
                                c.oid ) ),
                        E',\n' )
                FROM
                    pg_catalog.pg_constraint c
                WHERE
                    c.conrelid = $1
                    AND c.contype IN (
                        'c',
                        'p',
                        'u',
                        'x' )
                    AND coninhcount = 0
                    AND c.contype <> 't'
                    AND pg_get_constraintdef (
                        c.oid )
                    !~* 'CHECK \(edb_satisfies_partition\(' ) AS constraint_def
        FROM
            pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_class tc ON (
            c.reltoastrelid = tc.oid )
    WHERE
        c.oid = $1
        AND c.relkind IN (
            'u',
            't',
            'r' ) )
    d;
 $function$;



CREATE OR replace FUNCTION edb_ddl_api.get_table_indexes_ddl(table_oid OID,
                                                 on_tblspace BOOLEAN DEFAULT FALSE )
RETURNS TABLE(oid OID,ddl TEXT)
LANGUAGE sql
AS
$function$
SELECT
    c.oid,
    pg_catalog.REPLACE (
        pg_catalog.pg_get_indexdef (
            c.oid ),
        $1 ::REGCLASS::TEXT,
        (
            SELECT
                pg_catalog.quote_ident (
                    relname )
            FROM
                pg_catalog.pg_class
            WHERE
                oid = $1 ) )
    || CASE
        WHEN $2
        AND c.reltablespace <> 0 THEN (
            SELECT
                ' TABLESPACE ' || pg_catalog.quote_ident (
                    t.spcname )
            FROM
                pg_catalog.pg_tablespace t
            WHERE
                t.oid = c.reltablespace )
        ELSE ''
    END
FROM
    pg_catalog.pg_class c
    JOIN pg_catalog.pg_index i ON (
        c.oid = i.indexrelid )
WHERE
    c.relkind = 'i'
    AND c.oid NOT IN (
        SELECT
            conindid
        FROM
            pg_catalog.pg_constraint
        WHERE
            conindid = c.oid )
    AND i.indrelid = $1
$function$;


CREATE OR replace FUNCTION edb_ddl_api.get_table_triggers_ddl(table_oid OID)
RETURNS TABLE(oid OID, ddl TEXT)
LANGUAGE sql
AS
$function$
SELECT
    t.oid,
    pg_catalog.replace (
        pg_catalog.pg_get_triggerdef (
            t.oid,
            TRUE ),
        t.tgrelid::REGCLASS::TEXT,
        c.relname )
FROM
    pg_catalog.pg_trigger t,
    pg_class c
WHERE
    t.tgrelid = c.oid
    AND c.oid = $1
    AND (
        NOT t.tgisinternal
        OR (
            t.tgisinternal
            AND t.tgenabled = 'D' ) )
ORDER BY
    1
$function$;



CREATE OR REPLACE FUNCTION edb_ddl_api.copy_partition_table_structure(source_schema TEXT,
                                                                     source_table TEXT, 
                                                                     target_table TEXT,
                                                                     including_indexes boolean DEFAULT true,
                                                                     including_triggers BOOLEAN DEFAULT false)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS
$function$
DECLARE
  rec RECORD;
  src_tbl_wth_schm TEXT := pg_catalog.quote_ident(source_schema)||'.'||pg_catalog.quote_ident(source_table);
  table_name TEXT;
  sequence_name TEXT;
  column_name TEXT;
  message TEXT;
  detail TEXT;
  hint   TEXT;
  context TEXT;
  error_msg TEXT;
BEGIN

SELECT pg_catalog.replace(pg_catalog.replace(pg_catalog.quote_ident(s.relname), src_tbl_wth_schm, pg_catalog.quote_ident(target_table)),pg_catalog.quote_ident(source_table),pg_catalog.quote_ident(target_table)), pg_catalog.quote_ident (
        attname )
 INTO sequence_name, column_name
FROM
    pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_depend d ON c.oid = d.refobjid
    INNER JOIN pg_catalog.pg_attribute a ON (
            a.attrelid = c.oid
            AND a.attnum = d.refobjsubid ) 
    INNER JOIN pg_catalog.pg_class s ON (d.objid = s.oid AND s.relkind = 'S'::"char") 
    INNER JOIN pg_catalog.pg_namespace n ON (s.relnamespace = n.oid) 
    WHERE
        c.oid = src_tbl_wth_schm::REGCLASS::OID AND c.relkind = 'r'::"char" AND
        d.classid = 'pg_catalog.pg_class' ::pg_catalog.regclass
        AND d.refclassid = 'pg_catalog.pg_class' ::pg_catalog.regclass
        AND d.deptype = 'a';
   IF sequence_name <> '' AND sequence_name IS NOT NULL THEN
      EXECUTE 'CREATE SEQUENCE '|| sequence_name;
   END IF;

   /* build basic structure */
   SELECT edb_ddl_api.get_table_ddl(src_tbl_wth_schm::REGCLASS::OID) AS def INTO rec;

   EXECUTE pg_catalog.replace(pg_catalog.replace(rec.def, pg_catalog.quote_ident(src_tbl_wth_schm), pg_catalog.quote_ident(target_table)),pg_catalog.quote_ident(source_table), pg_catalog.quote_ident(target_table));

   /* if including indexes asked then build indexes */
   IF including_indexes THEN
      FOR rec IN SELECT ddl FROM edb_ddl_api.get_table_indexes_ddl(src_tbl_wth_schm::REGCLASS::OID)
      LOOP
          EXECUTE pg_catalog.replace(pg_catalog.replace(rec.ddl, src_tbl_wth_schm, pg_catalog.quote_ident(target_table)),pg_catalog.quote_ident(source_table),pg_catalog.quote_ident(target_table));
      END LOOP;
      FOR rec IN SELECT edb_ddl_api.get_table_indexes_ddl(partrelid).ddl from pg_catalog.edb_partition where partpdefid = (SELECT oid FROM edb_partdef WHERE pdefrel=src_tbl_wth_schm::REGCLASS::OID)
      LOOP
           EXECUTE pg_catalog.replace(pg_catalog.replace(rec.ddl, src_tbl_wth_schm, pg_catalog.quote_ident(target_table)),pg_catalog.quote_ident(source_table),pg_catalog.quote_ident(target_table));
      END LOOP;  
  END IF;

  /* if triggers asked then build triggers too */
  IF including_triggers THEN
      FOR rec IN SELECT ddl FROM edb_ddl_api.get_table_triggers_ddl(src_tbl_wth_schm::REGCLASS::OID)
      LOOP
          EXECUTE pg_catalog.replace(pg_catalog.replace(rec.ddl, src_tbl_wth_schm, pg_catalog.quote_ident(target_table)),pg_catalog.quote_ident(source_table),pg_catalog.quote_ident(target_table));
      END LOOP; 
      FOR rec IN SELECT edb_ddl_api.get_table_triggers_ddl(partrelid).ddl from pg_catalog.edb_partition where partpdefid = (SELECT oid FROM edb_partdef WHERE pdefrel=src_tbl_wth_schm::REGCLASS::OID)
      LOOP
           EXECUTE pg_catalog.replace(pg_catalog.replace(rec.ddl, src_tbl_wth_schm, pg_catalog.quote_ident(target_table)),pg_catalog.quote_ident(source_table),pg_catalog.quote_ident(target_table));
      END LOOP;  
  END IF;
  
  /* check if column has serial if serial is there then create new sequence and attach it with table and partition */

   IF sequence_name <> '' AND sequence_name IS NOT NULL THEN
      EXECUTE 'ALTER TABLE '||pg_catalog.quote_ident(target_table)||
              ' ALTER COLUMN '||column_name||' SET DEFAULT nextval('||pg_catalog.quote_literal(sequence_name::REGCLASS)||'::regclass)';
      FOR rec IN SELECT 'ALTER TABLE '||partrelid::REGCLASS::TEXT||
                        ' ALTER COLUMN '||column_name||' SET DEFAULT nextval('||
                                            pg_catalog.quote_literal(sequence_name::REGCLASS)||'::regclass)' as ddl FROM
                 pg_catalog.edb_partition where partpdefid = (SELECT oid FROM edb_partdef WHERE pdefrel=src_tbl_wth_schm::REGCLASS::OID)
      LOOP
        EXECUTE rec.ddl;
      END LOOP;
  END IF;

  RETURN true;
  EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS 
          message = MESSAGE_TEXT,
          detail = PG_EXCEPTION_DETAIL,
          hint = PG_EXCEPTION_HINT,
          context = PG_EXCEPTION_CONTEXT;
      SELECT CASE WHEN coalesce(message,'') != '' THEN 
                'MESSAGE: '||message 
                  ELSE '' END ||
             CASE WHEN coalesce(detail,'') != '' THEN
                E'\nDETAIL: '||detail 
                  ELSE '' END ||
             CASE WHEN coalesce(hint,'') != '' THEN
               E'\nHINT: '||hint
                  ELSE '' END ||
             CASE WHEN coalesce(context,'') != '' THEN
               E'\nCONTEXT: '||context
                  ELSE '' END 
             INTO error_msg;
             RAISE NOTICE 'ERROR: %',error_msg;
        RETURN false;
END;
$function$;

CREATE OR REPLACE FUNCTION edb_ddl_api.get_partition_table_ddl(table_oid OID,
                                                     including_indexes BOOLEAN DEFAULT false,
                                                     including_triggers BOOLEAN DEFAULT false)
RETURNS SETOF TEXT
LANGUAGE SQL
AS
$function$
   SELECT edb_ddl_api.get_table_ddl($1)
   UNION ALL
   SELECT edb_ddl_api.get_table_indexes_ddl($1).ddl WHERE including_indexes
   UNION ALL 
   SELECT edb_ddl_api.get_table_indexes_ddl(partrelid).ddl from pg_catalog.edb_partition where partpdefid = (SELECT oid FROM edb_partdef WHERE pdefrel=$1::regclass::OID AND including_indexes) 
   UNION ALL 
   SELECT edb_ddl_api.get_table_triggers_ddl($1).ddl WHERE including_triggers
   UNION ALL 
   SELECT edb_ddl_api.get_table_triggers_ddl(partrelid).ddl FROM pg_catalog.edb_partition where partpdefid = (SELECT oid FROM edb_partdef WHERE pdefrel=$1::regclass::OID AND including_triggers) 
$function$;
