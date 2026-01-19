DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_status_category.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Status_Category - BTEQ Script.
      Purpose: Builds the status category reference table used within the Business Objects AD-HOC Universe
 for reporting.
      Mod1: Creation of script on 7/26/2011. SW.
      Mod2: Added rename logic on 7/27/2011. SW.
      Mod3: Changed script for new DDL on 9/8/2011. SW.
	  Mod4: Add Coid  02/19/2016  JC
	Mod5:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod6: Removed _MV tables along with Insert/Select. Replaced with Merge
****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA154;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_status_category AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          msc.id AS status_category_id,
          max(msc.name) AS status_category_name,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_status_category AS msc
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON msc.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON sm.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(msc.name)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.status_category_id = z.status_category_id WHEN MATCHED THEN
UPDATE
SET status_category_name = substr(z.status_category_name, 1, 10),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        status_category_id,
        status_category_name,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.status_category_id, substr(z.status_category_name, 1, 10), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             status_category_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_status_category
      GROUP BY company_code,
               coid,
               status_category_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_status_category');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Status_Category');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;