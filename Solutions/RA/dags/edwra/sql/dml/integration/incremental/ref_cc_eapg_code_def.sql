DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_eapg_code_def.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Sean Wilson
          Date: 10/27/2014
          Name: Ref_CC_EAPG_Code_Def.sql
          Mod1: Removed filter for schema id 1 based on QA defect resolution on 12/5/2014 SW.
		  Mod2: PBI16687 - Change to table structure, re-define SQL on 6/18/2018 SW.
		  Mod3: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
		  Mod4: Removed DELETE/INSERT statements and replaced with MERGE logic. CB
*************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA259;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_code_def AS x USING
  (SELECT rccos.company_code,
          rccos.coid AS coid,
          cdef.code AS eapg_code,
          cdef.code_type AS eapg_type_code,
          cdef.version_id AS eapg_code_version_id,
          cdef.code_name AS eapg_code_name,
          cdef.short_name AS eapg_code_short_name,
          cdef.misc_char01 AS eapg_code_misc_desc,
          NULL AS eapg_code_misc_char_desc,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.code_def AS cdef
   INNER JOIN
     (SELECT max(cdf.code) AS code,
             cdf.schema_id,
             max(cdf.version_id) AS cdversnid
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.code_def AS cdf
      WHERE upper(cdf.code_type) LIKE 'EAPG%'
      GROUP BY upper(cdf.code),
               2) AS mdf ON upper(rtrim(cdef.code)) = upper(rtrim(mdf.code))
   AND cdef.schema_id = mdf.schema_id
   AND cdef.version_id = mdf.cdversnid
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = cdef.schema_id
   WHERE upper(cdef.code_type) LIKE 'EAPG%' ) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND upper(rtrim(x.eapg_code)) = upper(rtrim(z.eapg_code))
AND upper(rtrim(x.eapg_type_code)) = upper(rtrim(z.eapg_type_code)) WHEN MATCHED THEN
UPDATE
SET eapg_code_version_id = ROUND(z.eapg_code_version_id, 0, 'ROUND_HALF_EVEN'),
    eapg_code_name = z.eapg_code_name,
    eapg_code_short_name = substr(z.eapg_code_short_name, 1, 25),
    eapg_code_misc_desc = z.eapg_code_misc_desc,
    eapg_code_misc_char_desc = CAST(NULL AS STRING),
    source_system_code = substr(z.source_system_code, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        eapg_code,
        eapg_type_code,
        eapg_code_version_id,
        eapg_code_name,
        eapg_code_short_name,
        eapg_code_misc_desc,
        eapg_code_misc_char_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.eapg_code, z.eapg_type_code, ROUND(z.eapg_code_version_id, 0, 'ROUND_HALF_EVEN'), z.eapg_code_name, substr(z.eapg_code_short_name, 1, 25), z.eapg_code_misc_desc, CAST(NULL AS STRING), substr(z.source_system_code, 1, 1), z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             eapg_code,
             eapg_type_code
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_code_def
      GROUP BY company_code,
               coid,
               eapg_code,
               eapg_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_code_def');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_EAPG_Code_Def');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;