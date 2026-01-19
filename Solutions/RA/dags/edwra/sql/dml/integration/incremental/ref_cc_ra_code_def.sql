DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_ra_code_def.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_RA_Code_Def - BTEQ Script.
      Purpose: Builds the RA Code Def reference table used within the Business Objects AD-HOC Universe
 for reporting.
      Mod1: Creation of script on 8/8/2011. SW.
      Mod2: Changed script for new DDL on 9/12/2011, SW.
      Mod3: Updated max date created and max date update 4/7/2014 FY.
	  Mod4: Add Coid  02/19/2016  JC
	  Mod5: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod6: Removed _MV tables along with Insert/Select. Replaced with Merge PT
****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA221;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ra_code_def AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          rcd.id AS ra_code_def_id,
          max(rcd.code_type) AS ra_code_type,
          max(rcd.code) AS ra_code,
          max(rcd.short_description) AS ra_short_desc,
          max(rcd.description) AS ra_desc,
          max(CAST(rcd.date_created AS DATETIME)) AS create_date_time,
          max(CAST(rcd.date_updated AS DATETIME)) AS update_date_time,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_code_def AS rcd
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON rcd.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON sm.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(rcd.code_type),
            upper(rcd.code),
            upper(rcd.short_description),
            upper(rcd.description)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.ra_code_def_id = z.ra_code_def_id WHEN MATCHED THEN
UPDATE
SET ra_code_type = z.ra_code_type,
    ra_code = z.ra_code,
    ra_short_desc = z.ra_short_desc,
    ra_desc = z.ra_desc,
    create_date_time = z.create_date_time,
    update_date_time = z.update_date_time,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        ra_code_def_id,
        ra_code_type,
        ra_code,
        ra_short_desc,
        ra_desc,
        create_date_time,
        update_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.ra_code_def_id, z.ra_code_type, z.ra_code, z.ra_short_desc, z.ra_desc, z.create_date_time, z.update_date_time, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             ra_code_def_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ra_code_def
      GROUP BY company_code,
               coid,
               ra_code_def_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ra_code_def');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_RA_Code_Def');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;